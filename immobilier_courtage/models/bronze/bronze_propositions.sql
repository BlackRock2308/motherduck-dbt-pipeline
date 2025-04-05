{{
    config(
        materialized='view',
        schema='bronze',
        tags=['bronze', 'propositions']
    )
}}

WITH source AS (
    SELECT * FROM source.raw_propositions
),

renamed AS (
    SELECT
        -- Clés primaires et identifiants
        Id AS proposition_id,
        Opportunity__c AS opportunity_id,
        Partenaire__c AS partenaire_id,
        
        -- Informations sur le prêt
        TXHA__c AS taux_hors_assurance,
        DureePret_Mois__c AS duree_pret_mois,
        TauxAss__c AS taux_assurance,
        
        -- Informations métier
        Etape_Source__c AS etape_source,
        
        -- Données de traçabilité
        CreatedDate AS date_creation,
        _loaded_at,
        _source_file
    FROM source
),

clean_types AS (
    SELECT 
        proposition_id,
        opportunity_id,
        partenaire_id,
        CAST(taux_hors_assurance AS FLOAT) AS taux_hors_assurance,
        CAST(duree_pret_mois AS INTEGER) AS duree_pret_mois,
        CAST(taux_assurance AS FLOAT) AS taux_assurance,
        etape_source,
        CAST(date_creation AS TIMESTAMP) AS date_creation,
        _loaded_at,
        _source_file
    FROM renamed
)

SELECT * FROM clean_types