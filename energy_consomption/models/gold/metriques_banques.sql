{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'metrics', 'banques']
    )
}}

WITH propositions AS (
    SELECT * FROM {{ ref('propositions_enrichies') }}
),

-- Agrégation par banque (partenaire)
metriques_banques AS (
    SELECT
        partenaire_id,
        COUNT(*) AS nombre_propositions,
        COUNT(DISTINCT opportunity_id) AS nombre_opportunites,
        
        -- Taux moyens
        AVG(taux_hors_assurance) AS taux_moyen_hors_assurance,
        AVG(taux_assurance) AS taux_moyen_assurance,
        AVG(taux_effectif_global) AS taux_moyen_effectif_global,
        
        -- Analyse par segments
        AVG(CASE WHEN usage_bien = 'Résidence principale' THEN taux_hors_assurance ELSE NULL END) AS taux_moyen_residence_principale,
        AVG(CASE WHEN usage_bien = 'Investissement locatif' THEN taux_hors_assurance ELSE NULL END) AS taux_moyen_investissement_locatif,
        
        AVG(CASE WHEN categorie_professionnelle = 'Salarié du privé' THEN taux_hors_assurance ELSE NULL END) AS taux_moyen_salarie_prive,
        AVG(CASE WHEN categorie_professionnelle LIKE '%onctionnaire%' THEN taux_hors_assurance ELSE NULL END) AS taux_moyen_fonctionnaire,
        
        -- Répartition par durées
        COUNT(CASE WHEN duree_pret_mois <= 180 THEN 1 ELSE NULL END) AS count_duree_15ans_ou_moins,
        COUNT(CASE WHEN duree_pret_mois BETWEEN 181 AND 240 THEN 1 ELSE NULL END) AS count_duree_15_20ans,
        COUNT(CASE WHEN duree_pret_mois BETWEEN 241 AND 300 THEN 1 ELSE NULL END) AS count_duree_20_25ans,
        COUNT(CASE WHEN duree_pret_mois > 300 THEN 1 ELSE NULL END) AS count_duree_plus_25ans,
        
        -- Statistiques sur les montants
        MIN(montant_pret_principal) AS montant_min,
        MAX(montant_pret_principal) AS montant_max,
        AVG(montant_pret_principal) AS montant_moyen,
        
        -- Indicateurs de performance
        COUNT(CASE WHEN eligibilite = 'Éligible' THEN 1 ELSE NULL END) / NULLIF(COUNT(*), 0) * 100 AS pourcentage_eligibilite,
        
        -- Métriques temporelles
        MIN(date_creation) AS premiere_proposition,
        MAX(date_creation) AS derniere_proposition,
        
        -- Données de traçabilité
        MAX(_loaded_at) AS derniere_mise_a_jour
    FROM propositions
    GROUP BY partenaire_id
)

SELECT * FROM metriques_banques