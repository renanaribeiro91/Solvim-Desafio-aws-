const AWS = require('aws-sdk')

const dynamoDb = new AWS.DynamoDB.DocumentClient()
const s3 = new AWS.S3()

module.exports.getMetadata = async (event) => {

    const { s3objectkey } = event.pathParameters

    const params = {
        TableName: process.env.DYNAMODB_TABLE,
        Key: {
            "s3objectkey" : s3objectkey,
        }
    }

    const { Item } = await dynamoDb.get(params).promise()

    if (!Item) {
        return {
            statusCode: 400,
            body: `Não existe a key:${s3objectkey}`
        }
    }

    return {
        statusCode: 200,
        body: JSON.stringify(Item)
    }
}

const getImageFromBucket = async (key) => {
    try {
        return await s3.getObject({
            Bucket: process.env.BUCKET_NAME,
            Key: key
        }).promise()
    }
    catch(error) {
        console.log('Error no s3' + error)
    }
}

module.exports.extractMetadata = async (event) => {
    const sizeOf = require('image-size')

    for(const record of event.Records) {
        const { s3 } = record
        const { key, size } = s3.object
        const image = await getImageFromBucket(key)
        const { height, width, type } = sizeOf(image.Body)
        
        const params = {
            TableName: process.env.DYNAMODB_TABLE,
            Item: {
                dimensions: {
                    height,
                    width
                },
                type,
                s3objectkey: key.substr('8'), 
                size,
                createdAt: new Date().toISOString()
            }
        }
        
        await dynamoDb.put(params).promise()
    }
}

module.exports.getImage = async (event) => {
    const { s3objectkey } = event.pathParameters

    const key = `uploads/${s3objectkey}`
    const image = await getImageFromBucket(key)

    if (!image) {
        return {
            statusCode: 400,
            body: `Não existe a key:${s3objectkey}`
        }
    }

    return {
        statusCode: 200,
        headers: {
            "Content-Type": 'image/jpeg',
            "Content-Disposition": `attachment; filename=${s3objectkey}`
        },
        body: image.Body.toString('base64'),
        isBase64Encoded: true
    }
}

module.exports.infoImages = async (event) => { 
    const paramsDynamo = {
        TableName: process.env.DYNAMODB_TABLE,
    }

    const { Items } = await dynamoDb.scan(paramsDynamo).promise()

    const sortedImages = Items.sort((a,b) => a.size - b.size)
    const maxSize = sortedImages[sortedImages.length -1]
    const minSize = sortedImages[0]

    const paramsS3 = {
        Bucket: process.env.BUCKET_NAME,
        Prefix: 'uploads/'
    }

    const { Contents } = await s3.listObjectsV2(paramsS3).promise()

    Contents.shift()
    const arrayOfTypes = Contents.map(e => e.Key.split('.')[1])
    const imagesTypes = [...new Set(arrayOfTypes)]
    const quantityTypes = arrayOfTypes.reduce((cnt, cur) => (cnt[cur] = cnt[cur] + 1 || 1, cnt), {})

    return {
        statusCode: 200,
        body: `
        A imagem ${maxSize.s3objectkey} tem o maior tamanhado de ${maxSize.size}
        A imagem ${minSize.s3objectkey} tem o menor tamanhado de ${minSize.size}
        Os tipos de images salvas são: ${imagesTypes}
        A quantidade de cada Item são: ${JSON.stringify(quantityTypes)}
        `
    }
}
