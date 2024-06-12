{% macro create_aws_lava_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_lava_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::924682671219:role/lava-api-prod-rolesnowflakeudfsAF733095-beRq5VXtxXcA' api_allowed_prefixes = (
            'https://vnmhcb1q2j.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% else %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_lava_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/lava-api-stg-rolesnowflakeudfsAF733095-2GT4oQ1Dygg0' api_allowed_prefixes = (
            'https://9yzaxt6bk8.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
