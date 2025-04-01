{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'metrics', 'taux']
    )
}}

WITH propositions AS (
    SELECT * FROM {{ ref('propositions_enrichies') }}
),

-- Taux moyens par segmentation client
taux_par_profil AS (
    SELECT
        -- Dimensions de segmentation client
        categorie_professionnelle,
        contrat_travail,
        CASE
            WHEN age_emprunteur < 30 THEN 'Jeune'
            WHEN age_emprunteur BETWEEN 30 AND 45 THEN 'Milieu de vie'
            WHEN age_emprunteur > 45 THEN 'Senior'
            ELSE 'Non défini'
        END AS segment_age,
        
        CASE
            WHEN total_revenus <= 3000 THEN 'Revenus modestes'
            WHEN total_revenus BETWEEN 3001 AND 6000 THEN 'Revenus moyens'
            WHEN total_revenus > 6000 THEN 'Revenus élevés'
            ELSE 'Non défini'
        END AS segment_revenus,
        
        CASE
            WHEN taux_endettement < 20 THEN 'Faible endettement'
            WHEN taux_endettement BETWEEN 20 AND 33 THEN 'Endettement moyen'
            WHEN taux_endettement > 33 THEN 'Endettement élevé'
            ELSE 'Non défini'
        END AS segment_endettement,
        
        -- Dimensions produit
        type_projet,
        usage_bien,
        
        -- Statistiques sur les taux
        AVG(taux_hors_assurance) AS taux_moyen_hors_assurance,
        MIN(taux_hors_assurance) AS taux_min_hors_assurance,
        MAX(taux_hors_assurance) AS taux_max_hors_assurance,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY taux_hors_assurance) AS taux_q1,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY taux_hors_assurance) AS taux_median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY taux_hors_assurance) AS taux_q3,
        
        -- Statistiques sur les assurances
        AVG(taux_assurance) AS taux_moyen_assurance,
        
        -- Volume de propositions
        COUNT(*) AS nombre_propositions,
        COUNT(DISTINCT opportunity_id) AS nombre_opportunites,
        COUNT(DISTINCT partenaire_id) AS nombre_banques,
        
        -- Données de traçabilité
        MAX(date_creation) AS derniere_date_proposition,
        MAX(_loaded_at) AS derniere_mise_a_jour
    FROM propositions
    GROUP BY
        categorie_professionnelle,
        contrat_travail,
        CASE
            WHEN age_emprunteur < 30 THEN 'Jeune'
            WHEN age_emprunteur BETWEEN 30 AND 45 THEN 'Milieu de vie'
            WHEN age_emprunteur > 45 THEN 'Senior'
            ELSE 'Non défini'
        END,
        CASE
            WHEN total_revenus <= 3000 THEN 'Revenus modestes'
            WHEN total_revenus BETWEEN 3001 AND 6000 THEN 'Revenus moyens'
            WHEN total_revenus > 6000 THEN 'Revenus élevés'
            ELSE 'Non défini'
        END,
        CASE
            WHEN taux_endettement < 20 THEN 'Faible endettement'
            WHEN taux_endettement BETWEEN 20 AND 33 THEN 'Endettement moyen'
            WHEN taux_endettement > 33 THEN 'Endettement élevé'
            ELSE 'Non défini'
        END,
        type_projet,
        usage_bien
)

SELECT * FROM taux_par_profil