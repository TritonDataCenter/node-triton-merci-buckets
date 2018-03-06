var MorayBucketsInitializer = require('./lib/buckets-initializer');
var dataMigrationsLoader = require('./lib/data-migrations/loader');

module.exports = {
    loadDataMigrations: dataMigrationsLoader.loadMigrations,
    MorayBucketsInitializer: MorayBucketsInitializer
};