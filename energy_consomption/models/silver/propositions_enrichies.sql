{{
    config(
        materialized='table',
        schema='silver',
        tags=['silver', 'propositions']
    )
}}

WITH propositions AS (
    SELECT * FROM {{ ref('bronze_propositions') }}
),

opportunites AS (
    SELECT * FROM {{ ref('bronze_opportunites') }}
),

-- Enrichissement des propositions avec les données d'opportunités
propositions_enrichies AS (
    SELECT
        -- Clés de proposition
        p.proposition_id,
        p.opportunity_id,
        p.partenaire_id,
        
        -- Données de l'opportunité associée
        o.type_projet,
        o.usage_bien,
        o.montant_pret_principal,
        o.montant_apport_personnel,
        o.origine AS origine_opportunite,
        o.categorie_professionnelle,
        o.contrat_travail,
        o.total_revenus,
        o.taux_endettement,
        o.age_emprunteur,
        
        -- Données de la proposition
        p.taux_hors_assurance,
        p.taux_assurance,
        p.duree_pret_mois,
        p.etape_source,
        
        -- Calcul du coût total du crédit
        (p.taux_hors_assurance + p.taux_assurance) AS taux_effectif_global,
        
        -- Calcul de la mensualité approximative (formule simplifiée)
        (o.montant_pret_principal * (p.taux_hors_assurance/100/12) * 
         POWER(1 + (p.taux_hors_assurance/100/12), p.duree_pret_mois)) / 
         (POWER(1 + (p.taux_hors_assurance/100/12), p.duree_pret_mois) - 1) AS mensualite_approximative,
         
        -- Catégorisation des taux
        CASE
            WHEN p.taux_hors_assurance <= 3.5 THEN 'Bas'
            WHEN p.taux_hors_assurance BETWEEN 3.5 AND 4.5 THEN 'Moyen'
            WHEN p.taux_hors_assurance > 4.5 THEN 'Élevé'
            ELSE 'Non défini'
        END AS categorie_taux,
        
        -- Extraction de la source depuis l'étape
        CASE
            WHEN p.etape_source LIKE '%Source: SICM%' THEN 'SICM'
            WHEN p.etape_source LIKE '%Source: BPI%' THEN 'BPI'
            ELSE 'Autre'
        END AS source_proposition,
        
        -- Analyse du texte de l'étape
        CASE 
            WHEN p.etape_source LIKE '%Non éligible%' THEN 'Non éligible'
            WHEN p.etape_source LIKE '%éligible%' THEN 'Éligible'
            ELSE 'Indéterminé'
        END AS eligibilite,
        
        -- Dates
        p.date_creation,
        DATE_TRUNC('month', p.date_creation) AS mois_creation,
        
        -- Données de traçabilité
        p._loaded_at
    FROM propositions p
    LEFT JOIN opportunites o ON p.opportunity_id = o.opportunity_id
)

SELECT * FROM propositions_enrichies