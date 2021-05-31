'use strict';
const axios = require('axios');

module.exports.hello = async (event) => {

  const cache = await axios.get('http://localhost:8080/cache/dynamoDb/getTemplate')
  console.log(cache)
  return {
    statusCode: 200,
    body: JSON.stringify(
      {
        message: 'Go sServerless v1.0! Your function executed successfully!',
        input: event,
      },
      null,
      2
    ),
  };


  // Use this code if you don't use the http event with the LAMBDA-PROXY integration
  // return { message: 'Go Serverless v1.0! Your function executed successfully!', event };
};
