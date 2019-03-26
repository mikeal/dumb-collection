const cbor = require('dag-cbor-sync')
const sortBy = require('lodash.sortby')
const _cbor = cbor()

const mkopts = opts => {
  if (!opts) opts = {}
  if (!opts.encode) opts.encode = async data => _cbor.mkblock(data)
  if (!opts.decode) opts.decode = async block => _cbor.from(block)
  return opts
}

const decode = block => {
  // TODO: replace this with a fully dynamic codec selector
  if (block.cid.codec === 'dag-cbor') return _cbor.from(block)
}

class NotFound extends Error {
  get code () {
    return 404
  }
}
async function * read (node, start, end=null, opts=null) {
  opts = mkopts(opts)
  let i = 0
  while (i < node.children.length) {
    let [key, value] = node.children[i]
    if (typeof key === 'string') {
      if (key >= start) yield [key, value]
      if (end && key >= end) return
    } else if (Array.isArray(key)) {
      if (!CID.isCID(value)) throw new Error('Invalid Collection: slice reference is not CID')
      let [_start, _end] = key
      if (end && _start >= end) {
        return 
      } if (_end < start) {
        // noop
      } else {
        yield [key, value]
        /* TODO: this should be re-written to read all slices in parallel */
        yield * read(await decode(await get(value)), start, end, opts)
      }
    } else {
      throw new Error('Invalid Collection: unknown key value')
    }
    i += 1
  }
}

async function * patch (node, changes, config, opts) {
  opts = mkopts(opts)
  let i = 0
  changes = sortBy(changes, change => change[0])
  let change = changes.shift()
  while (change && i < node.children.length) {
    let [key, value] = node.children[i]
    if (key === change[0]) {
      node.children[i] = change
      change = changes.shift()
    } else if (key < change[0]) {
      node.children.splice(i+1, 0, change)
    }
    i++
  }
  if (change) changes.unshift(change)
  if (changes.length) {
    node.children = node.children.concat(changes)
  }
  if (node.children.length > config.maxSize) {
    // TODO: compact
  }
}

class CollectionInterface {
  constructor (block, opts) {
    this.opts = mkopts(opts)
    this.root = decode(block)
    this.config = this.root.then(node => node._config)
  }
  async get (key) {
    if (!this.opts.get) throw new Error('Must pass `get` option to collection interface for reads')
    let node = await this.root
    for await (let [_key, value] of read(node, key, null, this.opts)) {
      if (key === _key) return value
    }
    throw new NotFound(`Cannot find key: ${key}`)
  }
  async range (start, end) { 
    let self = this
    return (function * () {
      for await (let [key, value] of (read(await self.root), start, end, self.opts)) {
        if (typeof key === 'string') yield [key, value]
      }
    })()
  }
  async blocks (start, end) {
    return (function * () {
      for await (let [key, value] of (read(await self.root), start, end, self.opts)) {
        if (Array.isArray(key)) yield self.get(value)
      }
    })()
  }
  async patch (changes) {
    
  }
}
  



