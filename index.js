var aws = require('aws-sdk')
var through = require('through2')
var concat = require('concat-stream')
var concurrent = require('concurrent-stream')

module.exports = exports = new AWS()

function AWS() {
  this.s3 = {
    ls: s3ls,
    get: s3get
  }
  return this
}


function s3getCreator(bucketName) {
  return function() {
    var get = s3get(bucketName)
    return get
  }
}


function s3get(bucketName, key, concurrency) {
  var bucket = new aws.S3({ params: { Bucket: bucketName } })
  var nparallel = typeof key === 'number' ? key : concurrency
  var stream
  if (nparallel) {
    console.log(nparallel)
    var creator = s3getCreator(bucketName)
    stream = concurrent(nparallel, creator)
  }
  else {
    stream = through.obj(transform)
  }
  if (key && typeof key !== 'number') { stream.write(key); stream.end() }

  return stream

  function transform(obj, enc, next) {
    var self = this
    var params = { Key: obj.Key }
    bucket.getObject(params)
    .createReadStream()
    .pipe(concat(function(data) {
      self.push(data.toString())
      next()
    }))
  }
}


function s3ls(bucket, matchPrefix) {
  var bucket = new aws.S3({ params: { Bucket: bucket } })
  var stream = through.obj(transform)
  stream.write({})
  return stream

  function transform(obj, enc, next) {
    var self = this
    var options = {}
    if (matchPrefix) { options.Prefix = matchPrefix }
    paginate()
    function paginate(marker) {
      if (marker) { options.Marker = marker }
      bucket.listObjects(options, function(err, data) {
        if (err) { console.error(err) }
        data.Contents.forEach(function(c) { self.push(c) })
        if (data.IsTruncated) {
          var lastContent = data.Contents[data.Contents.length-1]
          paginate(lastContent.Key)
        }
      })
    }
    next()
  }
}
