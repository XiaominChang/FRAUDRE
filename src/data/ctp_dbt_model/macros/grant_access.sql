-- -------------------------------------------------------------------------------------------------
-- Author:      Avlok Bahri
-- Description: Create macro functions used in models                                                                                
-- -------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------
-- VERSIONS   DATE        WHO         	    DESCRIPTION
-- 1.0	      12/01/2024  Avlok Bahri       Create grant_access to grant access through Post_Hooks
-- -------------------------------------------------------------------------------------------------

{% macro grant_access(this) %}
    GRANT SELECT on TABLE {{this}} TO uproj_fraud_analytics_huddle;
    GRANT ALL ON TABLE {{ this }} TO uproj_fraud_analytics_huddle;
    Alter table {{ this }} OWNER TO dl_analytics_crt_role;
{% endmacro %}