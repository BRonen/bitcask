import { exec } from 'child_process'
import Bitcask from '~/index'

describe('Basic Operations', () => {
    beforeAll(() => exec('./clearStorage.sh'))

    it('should throw if writing in a read-only instance', async () => {
        const bitcask = Bitcask({
            path: './storage',
            writer: false,
            maxActiveFileSize: 5,
            maxFilesBeforeMerge: 3
        })

        await expect(async () => {
            await bitcask.put('key', 'value')
        }).rejects.toThrow('This instance isn\'t a writer instance')
    })

    it('should fold over every key and then return the accumulator', async () => {
        const bitcask = Bitcask({
            path: './storage',
            writer: true,
            maxActiveFileSize: 5,
            maxFilesBeforeMerge: 3
        })

        await bitcask.put('key test 1', '1')
        await bitcask.put('key test 2', '2')
        await bitcask.put('key test 3', '3')

        expect(
            await bitcask.fold<string>((_key, value, acc) => acc + value, '')
        ).toBe('123')
    })

    it('should not find any entry with key', async () => {
        const bitcask = Bitcask({
            path: './storage',
            writer: true,
            maxActiveFileSize: 5,
            maxFilesBeforeMerge: 3
        })

        const value = await bitcask.get('test')

        expect(value).toBeNull()
    })

    it('should list the created keys', async () => {
        const bitcask = Bitcask({
            path: './storage',
            writer: true,
            maxActiveFileSize: 5,
            maxFilesBeforeMerge: 3
        })

        const value = 'Test'

        await bitcask.put('key test 1', value)
        await bitcask.put('key test 2', value)
        await bitcask.put('key test 3', value)

        expect(await bitcask.listKeys()).toStrictEqual([
            'key test 1',
            'key test 2',
            'key test 3',
        ])
    })

    it('should find numeric value by key', async () => {
        const bitcask = Bitcask({
            path: './storage',
            writer: true,
            maxActiveFileSize: 5,
            maxFilesBeforeMerge: 3
        })

        const value = 9 as unknown as string

        await bitcask.put('test0', value)

        expect(Number(await bitcask.get('test0'))).toBe(value)
    })

    it('should store more than one key', async () => {
        const bitcask = Bitcask({
            path: './storage',
            writer: true,
            maxActiveFileSize: 5,
            maxFilesBeforeMerge: 3
        })

        const values = ['1', '2', '3', '4', '5', '6']

        await Promise.all(
            values.map(
                value => bitcask.put(`key ${value}`, value)
            )
        )

        await Promise.all(
            values.map(
                async value => expect(await bitcask.get(`key ${value}`)).toBe(value)
            )
        )
    })

    it('should find text value by key', async () => {
        const bitcask = Bitcask({
            path: './storage',
            writer: true,
            maxActiveFileSize: 5,
            maxFilesBeforeMerge: 3
        })

        const value = '123 456 789'

        await bitcask.put('test1', value)

        expect(await bitcask.get('test1')).toBe(value)
    })

    it('should delete value by key', async () => {
        const bitcask = Bitcask({
            path: './storage',
            writer: true,
            maxActiveFileSize: 5,
            maxFilesBeforeMerge: 3
        })

        const value = 'Lorem Ipsum'

        await bitcask.put('test', value)

        expect(await bitcask.get('test')).toBe(value)

        await bitcask.delete('test')

        expect(await bitcask.get('test')).toBeNull()
    })

    it('should read values stored on previous initializations', async () => {
        const value = 'Lorem Ipsum'

        {
            const bitcask = Bitcask({
                path: './storage',
                writer: true,
                maxActiveFileSize: 5,
                maxFilesBeforeMerge: 3
            })

            await bitcask.put('unique key', value)

            expect(await bitcask.get('unique key')).toBe(value)
        }
        {
            const bitcask = Bitcask({
                path: './storage',
                writer: false,
                maxActiveFileSize: 5,
                maxFilesBeforeMerge: 3
            })

            expect(await bitcask.get('unique key')).toBe(value)
        }
    })

    it('should merge previous files into a single one', async () => {
        const value = 'Lorem Ipsum'

        {
            const bitcask = Bitcask({
                path: './storage',
                writer: true,
                maxActiveFileSize: 5,
                maxFilesBeforeMerge: 3
            })

            await bitcask.put('unique key 1', value)
            await bitcask.put('unique key 2', value)
            await bitcask.put('unique key 3', value)

            expect(await bitcask.get('unique key 1')).toBe(value)
            expect(await bitcask.get('unique key 2')).toBe(value)
            expect(await bitcask.get('unique key 3')).toBe(value)
        }
        {
            const bitcask = Bitcask({
                path: './storage',
                writer: false,
                maxActiveFileSize: 5,
                maxFilesBeforeMerge: 3
            })

            expect(await bitcask.get('unique key 1')).toBe(value)
            expect(await bitcask.get('unique key 2')).toBe(value)
            expect(await bitcask.get('unique key 3')).toBe(value)
        }
    })

    it('should have a consistent value read', async () => {
        const value = 'Lorem Ipsum!'

        const bitcaskWriterInstance = Bitcask({
            path: './storage',
            writer: true,
            maxActiveFileSize: 5,
            maxFilesBeforeMerge: 3
        })

        const bitcaskReadOnlyInstance = Bitcask({
            path: './storage',
            writer: false,
            maxActiveFileSize: 5,
            maxFilesBeforeMerge: 3
        })

        await bitcaskWriterInstance.put('new unique key', value)

        expect(await bitcaskWriterInstance.get('new unique key')).toBe(value)
        expect(await bitcaskReadOnlyInstance.get('new unique key')).toBe(value)
    })
})
