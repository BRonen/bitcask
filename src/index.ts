import fs from 'fs'
import { RwLock, Ref } from 'rwlock-promise'
import crc32 from 'crc/crc32'
import { Buffer } from 'node:buffer'

type KeyDirHint = {
    fileId: number
    valueSize: number
    offset: number
    timestamp: number
}

type KeyDir = Record<string, KeyDirHint>

interface InstanceState {
  keyDir: KeyDir
  currentFile: number
  currentFileId: number
  directoryFilesNumber: number
  currentFileSize: number
}

type Value = string

interface BitCaskInstance {
  configs: {
    path: string
    writer: boolean
    maxActiveFileSize: number
    maxFilesBeforeMerge: number
  }
  state: RwLock<Ref<InstanceState>>
  delete(key: string): void
  put(key: string, value: Value): void
  get(key: string): Promise<null | Value>
  listKeys(): Promise<string[]>
  fold<T>(
    foldingCallback: (key: string, value: Value, acc: T) => T,
    acc: T,
  ): Promise<T>
  getValueByEntryHint(entryHint: KeyDirHint, key: string): Value
  merge(): void
}

const TOMBSTONE_VALUE = '__bitcask__tombstone__'

const checkHash = (hash: string, key: string, value: string) => {
    const newHash = crc32(`${key}${value}`).toString(16)

    if(hash === newHash) return

    throw new Error(
        `invalid checksum {${newHash} <-> ${hash}} of [${key} -> ${value}]`
    )
}

export const buildKeyDirFromFiles = (storagePath: string, directoryFiles: string[], ) => {
    const keyDir: KeyDir = {}

    for (const directoryFile of directoryFiles) {
        const fd = fs.openSync(`${storagePath}/${directoryFile}`, 'r')

        let offset = 0
        
        while(true) {
            const checksum = Buffer.alloc(8)
            const timestamp = Buffer.alloc(13)
            const keySize = Buffer.alloc(32)
            const valueSize = Buffer.alloc(32)
            
            fs.readSync(
                fd, checksum, 0,
                checksum.length,
                offset,
            )

            if(!checksum.readUInt8()) break
            
            fs.readSync(
                fd, timestamp, 0,
                timestamp.length,
                offset +
                checksum.length,
            )
            fs.readSync(
                fd, keySize, 0,
                keySize.length,
                offset +
                checksum.length +
                timestamp.length,
            )
            fs.readSync(
                fd, valueSize, 0,
                valueSize.length,
                offset +
                checksum.length +
                timestamp.length +
                keySize.length,
            )
    
            const key = Buffer.alloc(keySize.readUInt32LE())
    
            fs.readSync(
                fd, key, 0,
                keySize.readUInt32LE(),
                offset +
                checksum.length +
                timestamp.length +
                keySize.length +
                valueSize.length
            )
    
            const value = Buffer.alloc(valueSize.readUInt32LE())
    
            fs.readSync(
                fd, value, 0,
                valueSize.readUInt32LE(),
                offset +
                checksum.length +
                timestamp.length +
                keySize.length +
                valueSize.length +
                keySize.readUInt32LE()
            )
    
            const newChecksum = crc32(`${key.toString()}${value.toString()}`).toString(16)

            /*
            console.log(
                {
                    directoryFile: directoryFile,
                    checksum: checksum.toString(),
                    timestamp: timestamp.toString(),
                    keySize: keySize.readUInt32LE(),
                    valueSize: valueSize.readUInt32LE(),
                    key: key.toString(),
                    value: value.toString(),
                }
            )
            */

            if (newChecksum !== checksum.toString()) {
                throw new Error(`
                    invalid checksum {${newChecksum} <-> ${checksum.toString()}} of [${key.toString()} -> ${value.toString()}] on (${storagePath}/${directoryFile})
                `)
            }
            
            const fileId = Number(directoryFile.split('.')[2])
    
            const totalLength = (
                checksum.length +
                timestamp.length +
                keySize.length +
                valueSize.length +
                key.length +
                value.length
            )

            if (
                !keyDir[key.toString()] ||
                Number(timestamp.toString()) > keyDir[key.toString()].timestamp
            ) {
                keyDir[key.toString()] = {
                    fileId,
                    valueSize: valueSize.readUInt32LE(),
                    offset: offset,
                    timestamp: Number(timestamp.toString())
                }
            }

            offset += totalLength
        }

        fs.closeSync(fd)
    }

    return keyDir
}

const Bitcask = (configs: BitCaskInstance['configs']): BitCaskInstance => {
    const currentFilesInDirectory = fs.readdirSync(configs.path).filter(filePath => filePath.startsWith('bitcask.data'))
    
    const keyDir = buildKeyDirFromFiles(configs.path, currentFilesInDirectory)

    const lastFileId = currentFilesInDirectory.reduce(
        (acc, fileName) => {
            const [_name, _ext, id] = fileName.split('.')

            return Math.max(acc, Number(id))
        }, 0
    )

    const currentFile = fs.openSync(`${configs.path}/bitcask.data.${lastFileId + 1}`, 'a+')

    const state: RwLock<Ref<InstanceState>> = new RwLock(
        new Ref({
            keyDir,
            currentFile,
            currentFileId: lastFileId + 1,
            directoryFilesNumber: currentFilesInDirectory.length,
            currentFileSize: 0,
        })
    )

    return {
        configs,
        state,
        async delete (key) {
            return await this.put(key, TOMBSTONE_VALUE)
        },
        async put (key, value) {
            if(!this.configs.writer)
                throw new Error('This instance isn\'t a writer instance')

            // checksum => 8 bytes
            const checksum = Buffer.from(crc32(`${key}${value}`).toString(16))

            // timestamp => 13 bytes
            const timestamp = Date.now()
            const timestampBuffer = Buffer.alloc(13)
            timestampBuffer.write(String(timestamp))

            // key => {keySize} bytes
            // keySize => 32 bytes
            const keyBuffer = Buffer.from(key)
            const keySize = Buffer.alloc(32)
            keySize.writeUInt32LE(keyBuffer.length)

            // value => {valueSize} bytes
            // valueSize => 32 bytes
            const valueBuffer = Buffer.from(String(value))
            const valueSize = Buffer.alloc(32)
            valueSize.writeUInt32LE(valueBuffer.length)

            return await this.state.write(async stateRef => {
                const {
                    keyDir,
                    currentFile,
                    currentFileId,
                    currentFileSize,
                    directoryFilesNumber,
                } = stateRef.getValue()

                fs.appendFileSync(currentFile, checksum)
                fs.appendFileSync(currentFile, timestampBuffer)
                fs.appendFileSync(currentFile, keySize)
                fs.appendFileSync(currentFile, valueSize)
                fs.appendFileSync(currentFile, keyBuffer)
                fs.appendFileSync(currentFile, valueBuffer)

                /*console.log(
                    'checksum', `"${checksum.toString()}"`, checksum.length,
                )
                console.log(
                    'timestampBuffer', `"${timestampBuffer.toString()}"`, timestampBuffer.length,
                )
                console.log(
                    'keySize', `"${keySize.readUInt32LE()}"`, keySize.length,
                )
                console.log(
                    'valueSize', `"${valueSize.readUInt32LE()}"`, valueSize.length,
                )
                console.log(
                    'keyBuffer', `"${keyBuffer.toString()}"`, keyBuffer.length,
                )
                console.log(
                    'valueBuffer', `"${valueBuffer.toString()}"`, valueBuffer.length,
                )*/

                const totalLength = (
                    85 +
                    keySize.readUInt32LE() +
                    valueSize.readUInt32LE()
                )

                if (value === TOMBSTONE_VALUE) {
                    const { [key]: _currentEntry, ...newKeyDir } = keyDir

                    stateRef.setValue({
                        directoryFilesNumber,
                        currentFile,
                        currentFileId,
                        currentFileSize: currentFileSize + totalLength,
                        keyDir: {
                            ...newKeyDir,
                        }
                    })

                    return
                }

                stateRef.setValue({
                    directoryFilesNumber,
                    currentFile,
                    currentFileId,
                    currentFileSize: currentFileSize + totalLength,
                    keyDir: {
                        ...keyDir,
                        [key]: {
                            fileId: currentFileId,
                            valueSize: valueBuffer.length,
                            offset: currentFileSize,
                            timestamp
                        }
                    }
                })
            })
        },
        async get (key) {
            return await this.state.read(async stateRef => {
                const state = stateRef.getValue()

                const entryHint = state.keyDir[key]

                if (!entryHint) return null
    
                const result = this.getValueByEntryHint(entryHint, key)

                if (result === TOMBSTONE_VALUE) return null

                return result
            })
        },
        async listKeys () {
            return await this.state.read(async stateRef =>
                Object.keys(stateRef.getValue().keyDir)
            )
        },
        async fold (foldingCallback, acc) {
            return await this.state.read(async stateRef => 
                Object.entries(stateRef.getValue().keyDir)
                    .reduce(
                        (acc, [key, entryHint]) => foldingCallback(
                            key, this.getValueByEntryHint(entryHint, key), acc
                        ),
                        acc,
                    )
            )
        },
        getValueByEntryHint (entryHint: KeyDirHint, key: string) {
            const resultBuffer = Buffer.alloc(entryHint.valueSize)
            const hashBuffer = Buffer.alloc(8)
            const keySize = Buffer.from(key).length

            const fd = fs.openSync(`${this.configs.path}/bitcask.data.${entryHint.fileId}`, 'r')
            fs.readSync(
                fd, resultBuffer, 0, entryHint.valueSize, entryHint.offset + 85 + keySize
            )
            fs.readSync(
                fd, hashBuffer, 0, 8, entryHint.offset
            )
            fs.closeSync(fd)

            const value = resultBuffer.toString()

            checkHash(hashBuffer.toString(), key, value)

            return value.slice(-entryHint.valueSize)
        },
        async merge () {
            const currentFilesInDirectory = fs.readdirSync(configs.path).filter(filePath => filePath.startsWith('bitcask'))

            const oldKeyDir = buildKeyDirFromFiles(this.configs.path, currentFilesInDirectory)

            await this.state.write(async stateRef => {
                const { keyDir, ...state } = stateRef.getValue()
                const mergedKeyDir: KeyDir = {}

                for (const key of Object.keys(keyDir)) {
                    if(oldKeyDir[key] && oldKeyDir[key].timestamp > keyDir[key].timestamp)
                        mergedKeyDir[key] = oldKeyDir[key]
                    else
                        mergedKeyDir[key] = keyDir[key]
                }

                stateRef.setValue({
                    ...state, keyDir
                })
            })
        },
    }
}

export default Bitcask