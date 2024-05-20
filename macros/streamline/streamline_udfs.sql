{% macro create_udf_bulk_rest_api_v2() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(
        json OBJECT
    ) returns ARRAY api_integration = {% if target.name == "prod" %}
        aws_lava_api AS ''
    {% else %}
        aws_lava_api_dev AS 'https://9yzaxt6bk8.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api'
    {%- endif %};
{% endmacro %}
