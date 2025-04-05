{{
    config(
        materialized='table',
        schema='silver',
        tags=['silver', 'opportunities']
    )
}}

WITH opportunites AS (
    SELECT * FROM {{ ref('bronze_opportunites') }}
),

-- Enrichissement avec les métriques calculées
opportunites_enrichies AS (
    SELECT
        -- Données principales
        opportunity_id,
        date_creation,
        origine,
        etape,
        avancement,
        
        -- Informations emprunteur
        age_emprunteur,
        banque_principale,
        categorie_professionnelle,
        contrat_travail,
        situation_actuelle,
        
        -- Informations projet
        type_bien,
        type_projet,
        usage_bien,
        
        -- Informations financières
        montant_pret_principal,
        montant_apport_personnel,
        duree_souhaitee,
        taux_apport,
        taux_endettement,
        total_revenus,
        total_charges,
        revenu_residuel,
        
        -- Indicateurs commerciaux
        points_profil,
        esperance_gain_plateforme,
        total_propositions,
        
        -- Métriques calculées
        CASE 
            WHEN etape LIKE '1-%' THEN 'Découverte'
            WHEN etape LIKE '2-%' THEN 'Qualification'
            WHEN etape LIKE '3-%' THEN 'Proposition'
            WHEN etape LIKE '4-%' THEN 'Négociation'
            WHEN etape LIKE '5-%' THEN 'Contractualisation'
            WHEN etape LIKE '6-%' THEN 'Perdue'
            WHEN etape LIKE '7-%' THEN 'Gagnée'
            ELSE 'Autre'
        END AS phase_commerciale,
        
        -- Calcul du montant total du prêt (principal + autres)
        (COALESCE(montant_pret_principal, 0) + 
         COALESCE(montant_ptz, 0) + 
         COALESCE(montant_pel, 0) + 
         COALESCE(montant_cel, 0) + 
         COALESCE(montant_pret_relais, 0)) AS montant_total_pret,
        
        -- Segmentation clients
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
            WHEN contrat_travail LIKE '%CDI%' THEN 'Stable'
            WHEN contrat_travail LIKE '%CDD%' THEN 'Temporaire'
            WHEN contrat_travail LIKE '%Fonctionnaire%' THEN 'Stable'
            WHEN categorie_professionnelle LIKE '%indépendant%' THEN 'Indépendant'
            ELSE 'Autre'
        END AS segment_emploi,
        
        -- Indicateurs de conversion
        CASE
            WHEN etape LIKE '7-%' THEN 1
            ELSE 0
        END AS is_converted,
        
        CASE
            WHEN total_propositions > 0 THEN 1
            ELSE 0
        END AS has_propositions,
        
        -- Durées calculées
        DATE_TRUNC('month', date_creation) AS mois_creation,
        EXTRACT(YEAR FROM date_creation) AS annee_creation,
        
        -- Données de traçabilité
        _loaded_at
    FROM opportunites
)

SELECT * FROM opportunites_enrichies