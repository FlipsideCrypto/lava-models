{% macro add_database_or_schema_tags() %}
    {{ set_database_tag_value(
        'BLOCKCHAIN_NAME',
        'LAVA'
    ) }}
    {{ set_database_tag_value(
        'BLOCKCHAIN_TYPE',
        'IBC'
    ) }}
{% endmacro %}
