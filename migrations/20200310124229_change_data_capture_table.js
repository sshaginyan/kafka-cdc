
exports.up = function(knex) {
    return knex.schema.createTable('accounts', function (table) {
        table.increments();
        table.jsonb('cdc');
    });
};

exports.down = function(knex) {
    return knex.schema.dropTable('accounts');
};
