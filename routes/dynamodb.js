var express = require("express");
var router = new express.Router();
var AWS = require("aws-sdk");
var common = require("../common");
var config = common.config();
AWS.config.loadFromPath(config.aws_config_path);
var dynamodb = new AWS.DynamoDB({endpoint: new AWS.Endpoint(config.dynamodb_endpoint)});

var createConfigurationTable = function () {
    "use strict";

    var params = {
        AttributeDefinitions: [
            {
                AttributeName: "configuration",
                AttributeType: "S"
            }
        ],
        KeySchema: [
            {
                AttributeName: "configuration",
                KeyType: "HASH"
            }
        ],
        ProvisionedThroughput: {
            ReadCapacityUnits: 5,
            WriteCapacityUnits: 5
        },
        TableName: config.dynamo_table_configuration
    };
    dynamodb.createTable(params, function (err, data) {
        if (err) {
            console.log(err, err.stack);
        } else {
            console.log(data);
        }
    });
};

var checkConfigurationTable = function () {
    "use strict";

    var params = {
        TableName: config.dynamo_table_configuration
    };
    dynamodb.describeTable(params, function (err, data) {
        if (err) {
            console.log(err, err.stack);
            createConfigurationTable();
        } else {
            console.log(data);
        }
    });
};

var createRatingTable = function () {
    "use strict";

    var params = {
        AttributeDefinitions: [/* required */
            {
                AttributeName: "rating_id", /* required */
                AttributeType: "S" /* required */
            }
        ],
        KeySchema: [/* required */
            {
                AttributeName: "rating_id", /* required */
                KeyType: "HASH" /* required */
            }
        ],
        ProvisionedThroughput: {
            /* required */
            ReadCapacityUnits: 5, /* required */
            WriteCapacityUnits: 5 /* required */
        },
        TableName: config.dynamo_table_rating
    };
    dynamodb.createTable(params, function (err, data) {
        if (err) {
            console.log(err, err.stack);
        } else {
            console.log(data);
        }
    });
};

var checkRatingTable = function () {
    "use strict";

    var params = {
        TableName: config.dynamo_table_rating
    };
    dynamodb.describeTable(params, function (err, data) {
        if (err) {
            console.log(err, err.stack);
            createRatingTable();
        } else {
            console.log(data);
        }
    });
};

router.get("/", function (req, res, next) {
    "use strict";

    checkConfigurationTable();
    res.send(JSON.stringify({response: "OK"}));
});

module.exports = router;
