/**
 * Hello world as an OpenWhisk action.
 * From: https://github.com/apache/openwhisk/blob/master/docs/samples.md
*/
function main(params) {
    var name = params.name || 'World';
    return {payload:  'Hello, ' + name + '!'};
}

