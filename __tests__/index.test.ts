import Bitcask from '~/index'

describe('Basic Operations', () => {
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

    it('should find numeric value by key', async () => {
        const bitcask = Bitcask({
            path: './storage',
            writer: true,
            maxActiveFileSize: 5,
            maxFilesBeforeMerge: 3
        })

        const value = '9'

        await bitcask.put('test0', value)

        expect(await bitcask.get('test0')).toBe(String(value))
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
})
