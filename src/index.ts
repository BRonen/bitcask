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
  delete: (key: string) => void
  put: (key: string, value: Value) => void
  get: (key: string) => Promise<null | Value>
}

const TOMBSTONE_VALUE = '__bitcask__tombstone__'

export default (configs: BitCask['configs']): BitCask => {
    const keyDir = {}

    const currentFilesInDirectory = fs.readdirSync(configs.path).map(
        fileName => fileName.split('.')
    )

    const currentFileId = currentFilesInDirectory.reduce(
        (acc, [_name, _ext, id]) => id ? Math.max(acc, Number(id)) : 0, 0
    ) + 1

    const directoryFilesNumber = currentFilesInDirectory.length
    const currentFileSize = 0

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
            const checksum = crc32(`${key}${value}`).toString(16)
            const timestamp = Date.now()

            return await this.state.write(async stateRef => {
                const { keyDir, currentFileId, currentFileSize, directoryFilesNumber } = stateRef.getValue()

                const entry = `${checksum}${timestamp}${key.length}${String(value).length}${key}${value}`

                fs.appendFileSync(
                    `${this.configs.path}/bitcask.data.${currentFileId}`, entry
                )

                stateRef.setValue({
                    directoryFilesNumber,
                    currentFileId,
                    currentFileSize: currentFileSize + entry.length,
                    keyDir: {
                        ...keyDir,
                        [key]: {
                            fileId: currentFileId,
                            valueSize: String(value).length,
                            valuePosition: currentFileSize,
                            entryLength: entry.length,
                            timestamp
                        }
                    }
                })
                // console.log('current state:', { keyDir, ...state })
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
        }
    }
}
