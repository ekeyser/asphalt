var express = require("express");
var router = express.Router();

router.get("/", function (req, res, next) {
    "use strict";

    res.setHeader("content-type", "application/json");
    res.send(JSON.stringify({version: "1.0"}));
});

module.exports = router;