var env = require("./env.json");

exports.config = function () {
    "use strict";

    var node_env = process.env.NODE_ENV || "dev";
    return env[node_env];
};