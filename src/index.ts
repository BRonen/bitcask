import fs from 'fs'
import { RwLock, Ref } from 'rwlock-promise'
import crc32 from 'crc/crc32'
import { Buffer } from 'node:buffer'

type KeyDir = Record<string, {
  fileId: number
  valueSize: number
  valuePosition: number
  entryLength: number
  timestamp: number
}>

interface State {
  keyDir: KeyDir
  currentFileId: number
  directoryFilesNumber: number
  currentFileSize: number
}

type Value = string | number

interface BitCask {
  configs: {
    path: string
    writer: boolean
    maxActiveFileSize: number
    maxFilesBeforeMerge: number
  }
  state: RwLock<Ref<State>>
  delete(key: string): void
  put(key: string, value: Value): void
  get(key: string): Promise<null | Value>
}

const TOMBSTONE_VALUE = '__bitcask__tombstone__'

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

            if(newChecksum !== checksum.toString())
                throw new Error(
                    `invalid checksum {${newChecksum} <-> ${checksum.toString()}} of [${key.toString()} -> ${value.toString()}] on (${storagePath}/${directoryFile})`
                )
            
            const fileId = Number(directoryFile.split('.')[2])
    
            const totalLength = (
                checksum.length +
                timestamp.length +
                keySize.length +
                valueSize.length +
                key.length +
                value.length
            )

            offset += totalLength
    
            keyDir[key.toString()] = {
                fileId,
                valueSize: valueSize.readUInt32LE(),
                valuePosition: 0,
                entryLength: totalLength,
                timestamp: Number(timestamp.toString())
            }
        }

        fs.closeSync(fd)
    }

    return keyDir
}

const Bitcask = (configs: BitCask['configs']): BitCask => {
    const currentFilesInDirectory = fs.readdirSync(configs.path).filter(filePath => filePath.startsWith('bitcask'))
    
    const keyDir = buildKeyDirFromFiles(configs.path, currentFilesInDirectory)

    const currentFileId = currentFilesInDirectory.map(
        fileName => fileName.split('.')
    ).reduce(
        (acc, [_name, _ext, id]) => id ? Math.max(acc, Number(id)) : 0, 0
    ) + 1

    const directoryFilesNumber = currentFilesInDirectory.length
    const currentFileSize = 0

    console.log(keyDir)

    const state = new RwLock(
        new Ref({
            keyDir,
            currentFileId,
            directoryFilesNumber,
            currentFileSize
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

            // key => unknown bytes
            // keySize => 32 bytes
            const keyBuffer = Buffer.from(key)
            const keySize = Buffer.alloc(32)
            keySize.writeUInt32LE(keyBuffer.length)

            // value => unknown bytes
            // valueSize => 32 bytes
            const valueBuffer = Buffer.from(String(value))
            const valueSize = Buffer.alloc(32)
            valueSize.writeUInt32LE(valueBuffer.length)

            return await this.state.write(async stateRef => {
                const { keyDir, currentFileId, currentFileSize, directoryFilesNumber } = stateRef.getValue()
                const path = `${this.configs.path}/bitcask.data.${currentFileId}`
                fs.appendFileSync(
                    path, checksum
                )
                fs.appendFileSync(
                    path, timestampBuffer
                )
                fs.appendFileSync(
                    path, keySize
                )
                fs.appendFileSync(
                    path, valueSize
                )
                fs.appendFileSync(
                    path, keyBuffer
                )
                fs.appendFileSync(
                    path, valueBuffer
                )

                const totalLength = (
                    checksum.length +
                    timestampBuffer.length +
                    keySize.length +
                    valueSize.length +
                    keyBuffer.length +
                    valueBuffer.length
                )

                stateRef.setValue({
                    directoryFilesNumber,
                    currentFileId,
                    currentFileSize: currentFileSize + totalLength,
                    keyDir: {
                        ...keyDir,
                        [key]: {
                            fileId: currentFileId,
                            valueSize: String(value).length,
                            valuePosition: currentFileSize,
                            entryLength: totalLength,
                            timestamp
                        }
                    }
                })
            })
        },
        async get (key: string) {
            return await this.state.read(async stateRef => {
                const state = stateRef.getValue()

                const keyDir = state.keyDir[key]

                if (!keyDir) return null

                const resultBuffer = Buffer.alloc(keyDir.entryLength)

                // const file = fs.readFileSync(`${this.configs.path}/bitcask.data.${keyDir.fileId}`)
                const fd = fs.openSync(`${this.configs.path}/bitcask.data.${keyDir.fileId}`, 'r')
                fs.readSync(
                    fd, resultBuffer, 0, keyDir.entryLength, keyDir.valuePosition
                )
                fs.closeSync(fd)

                const result = resultBuffer.toString().slice(-keyDir.valueSize)

                if (result === TOMBSTONE_VALUE) return null

                return result
            })
        },
    }
}

export default Bitcask