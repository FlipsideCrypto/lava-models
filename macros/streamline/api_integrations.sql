{% macro create_aws_lava_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_lava_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::924682671219:role/lava-api-prod-rolesnowflakeudfsAF733095-ROp7MJlmcgxB' api_allowed_prefixes = (
            'https://v11xbhhwwd.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% else %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_lava_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/lava-api-stg-rolesnowflakeudfsAF733095-Q2ClQaocstGE' api_allowed_prefixes = (
            'https://38869wpr2a.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
