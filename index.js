const dotProp = require('dot-prop')
const XXHash = require('xxhash')
const fs = require('fs')
const es = require('event-stream')

const documents = {}
const index = {}
const availableKeys = new Set()

const streamFile = (filename, operation) => {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filename)
      .pipe(es.split())
      .pipe(es.map((line, cb) => cb(null, operation(line))))
      .on('error', reject)
      .on('end', resolve)
  })
}

const addDocumentToIndex = (document) => {
  const documentId = XXHash.hash(Buffer.from(JSON.stringify(document)), 0xCAFEBABE)
  if (documents[documentId]) {
    return false
  }
  documents[documentId] = document
  indexKeys(document, '', documentId)
  return true
}

const indexKeys = (obj, prefix = '', documentId) => {
  if (!obj) {
    return
  }
  if (Array.isArray(obj)) {
    obj.forEach((element, index) => {
      indexKeys(element, `${prefix}${index}.`, documentId)
    })
    return
  }
  Object.keys(obj).forEach(key => {
    if (typeof obj[key] === 'object') {
      indexKeys(obj[key], `${prefix}${key}.`, documentId)
    } else {
      const prefixedKey = `${prefix}${key}`
      availableKeys.add(prefixedKey)
      if (!index[prefixedKey]) {
        index[prefixedKey] = new Set()
      }
      index[prefixedKey].add(documentId)
    }
  })
}

const valuePasses = (value, clause) => {
  if (clause.operator === '==') {
    return value === clause.value
  } else if (clause.operator === '!=') {
    return value !== clause.value
  } else if (clause.operator === 'in') {
    return clause.value.includes(value)
  } else if (clause.operator === '!in') {
    return !(clause.value.includes(value))
  } else if (clause.operator === '~') {
    return value.indexOf(clause.value) !== -1
  } else if (clause.operator === '!~') {
    return value.indexOf(clause.value) === -1
  } else if (clause.operator === '>') {
    return value > clause.value
  } else if (clause.operator === '<') {
    return value < clause.value
  } else if (clause.operator === '>=') {
    return value >= clause.value
  } else if (clause.operator === '<=') {
    return value <= clause.value
  } else {
    throw new Error(`Invalid operator: ${clause.operator}`)
  }
}

const documentPasses = (document, clauses, operator) => {
  if (operator === 'AND') {
    return clauses.every(clause => valuePasses(dotProp.get(document, clause.key), clause))
  } else if (operator === 'OR') {
    return clauses.some(clause => valuePasses(dotProp.get(document, clause.key), clause))
  } else if (operator === 'NOT') {
    return !(clauses.some(clause => valuePasses(dotProp.get(document, clause.key), clause)))
  } else {
    throw new Error(`Invalid operator: ${operator}`)
  }
}

const search = (clauses, operator) => {
  return Object
    .keys(documents)
    .filter(documentId => documentPasses(documents[documentId], clauses, operator))
    .map(documentId => Object.assign({}, documents[documentId], {documentId: documentId}))
}

(async () => {
  let counter = 0
  const filename = 'logs-2019-01-25.log'

  await streamFile(filename, (line) => {
    try {
      addDocumentToIndex(JSON.parse(line))
      counter += 1
    } catch (err) {
      console.error(err.message)
    }
  })

  const results = search([
    {
      key: 'body.shipments.0.customsItems.0.quantity',
      operator: '==',
      value: 3
    }
  ], 'AND')

  console.log(results)
})()
