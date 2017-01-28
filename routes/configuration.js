var express = require("express");
var router = new express.Router();
var AWS = require("aws-sdk");
var common = require("../common");
var config = common.config();
AWS.config.loadFromPath(config.aws_config_path);
var dynamodb = new AWS.DynamoDB({endpoint: new AWS.Endpoint(config.dynamodb_endpoint)});
var util = require("util");
var sKey;
var jsonContent;

var postConfiguration = function () {
    "use strict";

    var objParams = {
        TableName: config.dynamo_table_configuration,
        Item: {
            "configuration": {
                S: sKey
            },
            "content": {
                S: JSON.stringify(jsonContent)
            }
        }
    };

    dynamodb.putItem(objParams, function (err, data) {
        if (err) {
            console.warn(err, err.stack);
        } else {
            console.log(data);
        }
    });
};

router.post("/", function (req, res, next) {
    "use strict";

    console.log(util.inspect(req.body, {showHidden: true, depth: null}));
    sKey = req.body.key;
    jsonContent = req.body.configuration;

    postConfiguration();
    // if (req.headers["x-amz-sns-message-type"] === "SubscriptionConfirmation") {
    //     var tools = require("../tools.js");
    //     tools.confirmSNSSubscription(req, AWS);
    // } else {
    //     retrieve();
    // }
    res.send(JSON.stringify({response: "OK"}));
});

module.exports = router;
