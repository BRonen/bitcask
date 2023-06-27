import { sum } from '~/index'

describe('initial module', () => {
  it('should sum two numbers', () => {
    expect(sum(1, 2)).toBe(3)
  })
})
