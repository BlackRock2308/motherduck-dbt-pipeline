{{
    config(
        materialized='view',
        schema='bronze',
        tags=['bronze', 'opportunities']
    )
}}

WITH source AS (
    SELECT * FROM source.raw_opportunites
),

renamed AS (
    SELECT
        -- Clés primaires et identifiants
        Id AS opportunity_id,
        RecordTypeId AS record_type_id,
        Id_ApporteurWeb__c AS web_provider_id,
        
        -- Informations sur l'emprunteur
        Age_emprunteur__c AS age_emprunteur,
        BanquePrincipaleEmp__c AS banque_principale,
        TechMail_CategorieProfessionnelleEmpru__c AS categorie_professionnelle,
        TechMail_ContratDeTravailEmprunteur__c AS contrat_travail,
        TechMail_CategorieProfessionnelleCoEmpru__c AS categorie_professionnelle_coemprunteur,
        TechMail_ContratDeTravailCoEmprunteur__c AS contrat_travail_coemprunteur,
        SituActu__c AS situation_actuelle,
        
        -- Informations sur le projet immobilier
        TypBien__c AS type_bien,
        TypProj__c AS type_projet,
        UsagBien__c AS usage_bien,
        Deja_souscrit_credit_immo__c AS deja_souscrit_credit,
        Connaissances_en_immobilier__c AS connaissances_immobilier,
        
        -- Informations financières
        MontPretPricip__c AS montant_pret_principal,
        MontPretTxZero__c AS montant_ptz,
        MontPretPel__c AS montant_pel,
        MontPretCEL__c AS montant_cel,
        MontPretRel__c AS montant_pret_relais,
        MontAppPerso__c AS montant_apport_personnel,
        MontEstimTravaux__c AS montant_travaux,
        DurSouhaitePret__c AS duree_souhaitee,
        MensuSouhaitePret__c AS mensualite_souhaitee,
        Taux_d_apport__c AS taux_apport,
        TxEndetApres__c AS taux_endettement,
        TotRev__c AS total_revenus,
        TotCharges__c AS total_charges,
        Residuel__c AS revenu_residuel,
        
        -- Informations sur l'opportunité
        CreatedDate AS date_creation,
        Origine__c AS origine,
        StageName AS etape,
        Avancement__c AS avancement,
        Nombre_de_banques_consultees__c AS nombre_banques_consultees,
        A_une_proposition_de_sa_banque__c AS a_proposition_banque,
        TotalProposition__c AS total_propositions,
        PropFinal__c AS proposition_finale,
        
        -- Métriques commerciales
        Points_profil__c AS points_profil,
        Points_profil_initiaux__c AS points_profil_initiaux,
        Points_profil_franchise__c AS points_profil_franchise,
        Points_profil_initiaux_franchise__c AS points_profil_initiaux_franchise,
        Esperance_de_gain_plateforme__c AS esperance_gain_plateforme,
        Esperance_de_gain_plateforme_initiale__c AS esperance_gain_plateforme_initiale,
        Esperance_de_gain_franchise__c AS esperance_gain_franchise,
        Esperance_de_gain_franchise_initiale__c AS esperance_gain_franchise_initiale,
        HonorairMTX__c AS honoraires,
        
        -- Données de traçabilité
        _loaded_at,
        _source_file
    FROM source
),

clean_types AS (
    SELECT 
        opportunity_id,
        record_type_id,
        web_provider_id,
        CAST(age_emprunteur AS INTEGER) AS age_emprunteur,
        banque_principale,
        categorie_professionnelle,
        contrat_travail,
        categorie_professionnelle_coemprunteur,
        contrat_travail_coemprunteur,
        situation_actuelle,
        type_bien,
        type_projet,
        usage_bien,
        CAST(deja_souscrit_credit AS BOOLEAN) AS deja_souscrit_credit,
        CAST(connaissances_immobilier AS FLOAT) AS connaissances_immobilier,
        CAST(montant_pret_principal AS FLOAT) AS montant_pret_principal,
        CAST(montant_ptz AS FLOAT) AS montant_ptz,
        CAST(montant_pel AS FLOAT) AS montant_pel,
        CAST(montant_cel AS FLOAT) AS montant_cel,
        CAST(montant_pret_relais AS FLOAT) AS montant_pret_relais,
        CAST(montant_apport_personnel AS FLOAT) AS montant_apport_personnel,
        CAST(montant_travaux AS FLOAT) AS montant_travaux,
        CAST(duree_souhaitee AS FLOAT) AS duree_souhaitee,
        CAST(mensualite_souhaitee AS FLOAT) AS mensualite_souhaitee,
        CAST(taux_apport AS FLOAT) AS taux_apport,
        CAST(taux_endettement AS FLOAT) AS taux_endettement,
        CAST(total_revenus AS FLOAT) AS total_revenus,
        CAST(total_charges AS FLOAT) AS total_charges,
        CAST(revenu_residuel AS FLOAT) AS revenu_residuel,
        CAST(date_creation AS TIMESTAMP) AS date_creation,
        origine,
        etape,
        CAST(avancement AS FLOAT) AS avancement,
        CAST(nombre_banques_consultees AS FLOAT) AS nombre_banques_consultees,
        CAST(a_proposition_banque AS BOOLEAN) AS a_proposition_banque,
        CAST(total_propositions AS FLOAT) AS total_propositions,
        proposition_finale,
        CAST(points_profil AS FLOAT) AS points_profil,
        CAST(points_profil_initiaux AS FLOAT) AS points_profil_initiaux,
        CAST(points_profil_franchise AS FLOAT) AS points_profil_franchise,
        CAST(points_profil_initiaux_franchise AS FLOAT) AS points_profil_initiaux_franchise,
        CAST(esperance_gain_plateforme AS FLOAT) AS esperance_gain_plateforme,
        CAST(esperance_gain_plateforme_initiale AS FLOAT) AS esperance_gain_plateforme_initiale,
        CAST(esperance_gain_franchise AS FLOAT) AS esperance_gain_franchise,
        CAST(esperance_gain_franchise_initiale AS FLOAT) AS esperance_gain_franchise_initiale,
        CAST(honoraires AS FLOAT) AS honoraires,
        _loaded_at,
        _source_file
    FROM renamed
)

SELECT * FROM clean_types