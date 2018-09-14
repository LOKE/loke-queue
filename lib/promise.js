module.exports =
  typeof Promise === "undefined" ? require("es6-promise").Promise : Promise;
