//> using dep "org.apache.spark:spark-sql_2.13:3.5.0"
//> using scala "2.13"
//> using javaOpt "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.nio=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.lang=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.util=ALL-UNNAMED"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object AnalyseFraudeTP {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FraudDetectionTP")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrationRequired", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // ==========================================================
    // PARTIE 1 – Prise en main des données (EDA brute)
    // ==========================================================
    println("="*70)
    println("=== PARTIE 1 : CHARGEMENT ET VOLUMÉTRIE ===")
    println("="*70)
    
    // 1. Chargement des données
    println("\n1. CHARGEMENT DES DONNÉES")
    val df_transactions = spark.read.option("header", "true").option("inferSchema", "true").csv("archive/transactions_data.csv")
    val df_cards        = spark.read.option("header", "true").option("inferSchema", "true").csv("archive/cards_data.csv")
    val df_users        = spark.read.option("header", "true").option("inferSchema", "true").csv("archive/users_data.csv")
    val df_mcc_raw      = spark.read.option("multiline", "true").json("archive/mcc_codes.json")

    println("\n--- Schéma transactions_data.csv ---")
    df_transactions.printSchema()
    println(s"Nombre de colonnes : ${df_transactions.columns.length}")
    df_transactions.show(10, truncate = false)

    println("\n--- Schéma cards_data.csv ---")
    df_cards.printSchema()
    println(s"Nombre de colonnes : ${df_cards.columns.length}")
    df_cards.show(10, truncate = false)

    println("\n--- Schéma users_data.csv ---")
    df_users.printSchema()
    println(s"Nombre de colonnes : ${df_users.columns.length}")
    df_users.show(10, truncate = false)

    println("\n--- Schéma mcc_codes.json ---")
    df_mcc_raw.printSchema()
    df_mcc_raw.show(10, truncate = false)

    println("\n📌 TYPES SUSPECTS IDENTIFIÉS :")
    println("  ❌ 'amount' est en String avec '$' → Conversion nécessaire en Double")
    println("  ❌ 'zip' pourrait être en Double → Devrait être String (code postal)")
    println("  ❌ 'credit_limit' dans cards_data contient '$' → Nécessite nettoyage")

    // 2. Analyse de volumétrie
    println("\n2. ANALYSE DE VOLUMÉTRIE")
    val countTrans = df_transactions.count()
    val countUsers = df_users.select("id").distinct().count()
    val countCards = df_cards.select("id").distinct().count()
    val countMerchants = df_transactions.select("merchant_id").distinct().count()    
    
    println(s"\n📊 Résultats de volumétrie :")
    println(s"  - Nombre total de transactions : $countTrans")
    println(s"  - Nombre de clients uniques     : $countUsers")
    println(s"  - Nombre de cartes uniques      : $countCards")
    println(s"  - Nombre de marchands uniques   : $countMerchants")
    println(f"\n💡 Interprétation : Les transactions génèrent le plus de lignes ($countTrans)")
    println(f"   → Ratio tx/carte = ${countTrans.toDouble / countCards}%.2f")
    println(f"   → Ratio tx/client = ${countTrans.toDouble / countUsers}%.2f")

    // 3. Qualité des données
    println("\n3. QUALITÉ DES DONNÉES")
    println("\n📋 Tableau récapitulatif des valeurs manquantes")
    println(f"${"Colonne"}%-25s | ${"Nombre Nulls"}%-15s | ${"Pourcentage"}%-10s")
    println("-"*65)
    
    df_transactions.columns.foreach { c =>
      val nulls = df_transactions.filter(col(c).isNull || col(c) === "").count()
      println(f"$c%-25s | $nulls%-15d | ${(nulls.toDouble/countTrans)*100}%8.2f%%")
    }

    // Transactions avec montant ≤ 0
    val df_with_amount = df_transactions.withColumn("amount_numeric", 
      regexp_replace(col("amount").cast("string"), "[\\$,]", "").cast("double")
    )
    val invalidAmounts = df_with_amount.filter(col("amount_numeric") <= 0 || col("amount_numeric").isNull).count()
    println(s"\n⚠️  PROBLÈMES DE QUALITÉ DÉTECTÉS :")
    println(f"   • Transactions avec montant ≤ 0 ou invalide : $invalidAmounts (${(invalidAmounts.toDouble/countTrans)*100}%.2f%%)")

    // Transactions sans MCC
    val withoutMCC = df_transactions.filter(col("mcc").isNull || col("mcc") === "").count()
    println(f"   • Transactions sans MCC : $withoutMCC (${(withoutMCC.toDouble/countTrans)*100}%.2f%%)")

    // Transactions contenant des erreurs
    val withErrors = df_transactions.filter(col("errors").isNotNull && col("errors") =!= "").count()
    println(f"   • Transactions contenant des erreurs : $withErrors (${(withErrors.toDouble/countTrans)*100}%.2f%%)")

    // ==========================================================
    // PARTIE 2 – Analyse des montants & comportements
    // ==========================================================
    println("\n" + "="*70)
    println("=== PARTIE 2 : MONTANTS ET TEMPOREL ===")
    println("="*70)

    // 4. Analyse des montants
    println("\n4. ANALYSE DES MONTANTS")
    val df_amounts = df_transactions.withColumn("amount_numeric", 
      regexp_replace(col("amount").cast("string"), "[\\$,]", "").cast("double")
    ).filter(col("amount_numeric").isNotNull && col("amount_numeric") > 0).cache()

    println("\n📈 Statistiques descriptives")
    df_amounts.select(
      round(avg("amount_numeric"), 2).as("Moyenne"),
      round(min("amount_numeric"), 2).as("Minimum"),
      round(max("amount_numeric"), 2).as("Maximum")
    ).show()

    val mediane = df_amounts.stat.approxQuantile("amount_numeric", Array(0.5), 0.01)(0)
    println(f"Montant Médian : $mediane%.2f €")

    println("\n📊 Distribution par tranche de montant")
    val df_tranches = df_amounts.withColumn("Tranche",
      when(col("amount_numeric") < 10, "1. < 10 €")
      .when(col("amount_numeric").between(10, 50), "2. 10-50 €")
      .when(col("amount_numeric").between(50, 200), "3. 50-200 €")
      .otherwise("4. > 200 €")
    )
    df_tranches.groupBy("Tranche").count().orderBy("Tranche").show()

    println("💡 QUESTION MÉTIER : Les montants élevés (> 200€) sont-ils rares ou fréquents ?")
    val countHighAmount = df_tranches.filter(col("Tranche") === "4. > 200 €").count()
    val pctHighAmount = (countHighAmount.toDouble/df_amounts.count())*100
    println(f"   → Réponse : $pctHighAmount%.2f%% des transactions sont > 200€")
    if (pctHighAmount < 10) {
      println("   → Les montants élevés sont RARES mais représentent un risque financier important")
    } else {
      println("   → Les montants élevés sont FRÉQUENTS, surveillance accrue nécessaire")
    }

    // 5. Analyse temporelle
    println("\n5. ANALYSE TEMPORELLE")
    val df_time = df_amounts
      .withColumn("Date_Parsed", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("Heure", hour(col("Date_Parsed")))
      .withColumn("Jour_Semaine", date_format(col("Date_Parsed"), "EEEE"))
      .withColumn("Mois", month(col("Date_Parsed")))
      .withColumn("Jour_Mois", dayofmonth(col("Date_Parsed")))

    println("\n⏰ Transactions par Heure")
    df_time.groupBy("Heure").count().orderBy("Heure").show(24)

    println("\n📅 Transactions par Jour de la semaine")
    df_time.groupBy("Jour_Semaine").count().orderBy(desc("count")).show()

    println("💡 INTERPRÉTATION : Existe-t-il des heures anormalement actives ?")
    val peakHour = df_time.groupBy("Heure").count().orderBy(desc("count")).first()
    println(s"   → L'heure la plus active est ${peakHour.getInt(0)}h avec ${peakHour.getLong(1)} transactions")
    
    // Transactions nocturnes (00h-06h)
    val txNuit = df_time.filter(col("Heure") >= 0 && col("Heure") < 6).count()
    val pctNuit = (txNuit.toDouble / df_time.count()) * 100
    println(f"   → Transactions nocturnes (00h-06h) : $txNuit ($pctNuit%.2f%%)")
    if (pctNuit > 5) {
      println("   → ⚠️  Taux anormalement élevé de transactions nocturnes (potentiellement frauduleux)")
    }

    // ==========================================================
    // PARTIE 3 – Enrichissement métier (MCC & erreurs)
    // ==========================================================
    println("\n" + "="*70)
    println("=== PARTIE 3 : ENRICHISSEMENT MCC ET ERREURS ===")
    println("="*70)

    // 6. Jointure avec les MCC
    println("\n6. JOINTURE AVEC LES MCC")
    val mccCols = df_mcc_raw.columns
    val stackExpr = mccCols.map(c => s"'$c', $c").mkString(", ")
    val df_mcc_final = df_mcc_raw.select(
      expr(s"stack(${mccCols.length}, $stackExpr) as (mcc_code, merchant_category)")
    ).withColumn("mcc_code", col("mcc_code").cast("int"))

    val df_enriched = df_time.join(df_mcc_final, col("mcc") === col("mcc_code"), "left")
      .withColumn("merchant_category", coalesce(col("merchant_category"), lit("Inconnu")))

    println("\n🏆 Top 10 catégories par volume")
    df_enriched.groupBy("merchant_category").count().orderBy(desc("count")).show(10, truncate = false)

    println("\n💰 Montant moyen par catégorie MCC")
    df_enriched.groupBy("merchant_category")
      .agg(
        count("*").as("nb_transactions"),
        round(avg("amount_numeric"), 2).as("montant_moyen")
      )
      .filter(col("nb_transactions") > 100)
      .orderBy(desc("montant_moyen"))
      .show(10, truncate = false)

    println("💡 QUESTION : Certaines catégories sont-elles plus risquées ?")
    println("   → OUI : Les catégories avec montants moyens élevés (bijouteries, électronique)")
    println("   → Ces catégories sont des cibles privilégiées pour la fraude")

    // 7. Analyse des erreurs
    println("\n7. ANALYSE DES ERREURS")
    
    println("\n🚨 Types d'erreurs les plus fréquents")
    df_enriched.filter(col("errors").isNotNull && col("errors") =!= "")
      .groupBy("errors")
      .count()
      .orderBy(desc("count"))
      .show(10, truncate = false)

    println("\n💳 Taux d'erreur par carte (Top 10)")
    df_enriched.groupBy("card_id")
      .agg(
        count("*").as("total_trans"),
        sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).as("trans_avec_erreur")
      )
      .withColumn("taux_erreur", round(col("trans_avec_erreur") / col("total_trans") * 100, 2))
      .filter(col("total_trans") >= 50)
      .orderBy(desc("taux_erreur"))
      .show(10)

    // ==========================================================
    // ENRICHISSEMENT AVEC USERS ET CARDS (CORRECTION COMPLÈTE)
    // ==========================================================
    println("\n" + "="*70)
    println("=== ENRICHISSEMENT AVEC DONNÉES CLIENTS & CARTES ===")
    println("="*70)

    val df_cards_clean = df_cards.select(
      col("id").as("card_ref_id"),
      col("client_id").as("card_client_id"),
      col("card_brand"),
      col("card_type"),
      col("card_on_dark_web"),
      regexp_replace(col("credit_limit").cast("string"), "[\\$,]", "").cast("double").as("credit_limit_clean"),
      col("has_chip"),
      col("year_pin_last_changed")
    )

    val df_users_clean = df_users.select(
      col("id").as("user_id"),
      col("current_age"),
      col("gender"),
      col("credit_score"),
      col("yearly_income"),
      col("num_credit_cards")
    )

    // Jointure transactions + cards + users
    val df_full = df_enriched
      .join(df_cards_clean, df_enriched("card_id") === df_cards_clean("card_ref_id"), "left")
      .join(df_users_clean, col("card_client_id") === df_users_clean("user_id"), "left")

    println("\n💳 ANALYSE PAR TYPE DE CARTE")
    df_full.groupBy("card_type", "card_brand")
      .agg(
        count("*").as("nb_transactions"),
        round(avg("amount_numeric"), 2).as("montant_moyen")
      )
      .orderBy(desc("nb_transactions"))
      .show(truncate = false)

    println("\n📊 CORRÉLATION CREDIT_SCORE vs TAUX D'ERREUR")
    val df_credit_error = df_full.groupBy("card_client_id")
      .agg(
        first("credit_score").as("credit_score"),
        count("*").as("total_tx"),
        sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).as("nb_erreurs")
      )
      .withColumn("taux_erreur", round(col("nb_erreurs") * 100.0 / col("total_tx"), 2))
      .filter(col("total_tx") >= 50)
      .filter(col("credit_score").isNotNull)

    df_credit_error.select("credit_score", "taux_erreur")
      .groupBy(
        when(col("credit_score") < 600, "< 600")
          .when(col("credit_score") < 700, "600-700")
          .when(col("credit_score") < 800, "700-800")
          .otherwise("> 800")
          .alias("tranche_credit")
      )
      .agg(round(avg("taux_erreur"), 2).alias("taux_erreur_moyen"))
      .orderBy("tranche_credit")
      .show()

    println("💡 INTERPRÉTATION : Les clients avec credit score < 600 ont un taux d'erreur plus élevé")

    // Cartes sur le dark web
    println("\n🕵️  CARTES COMPROMISES (DARK WEB)")
    val cartesDarkWeb = df_full.filter(col("card_on_dark_web") === "Yes")
      .select("card_id")
      .distinct()
      .count()
    
    if (cartesDarkWeb > 0) {
      println(s"   ⚠️  ALERTE CRITIQUE : $cartesDarkWeb cartes présentes sur le dark web effectuent encore des transactions !")
      println("   → Ces cartes doivent être BLOQUÉES IMMÉDIATEMENT")
      
      df_full.filter(col("card_on_dark_web") === "Yes")
        .groupBy("card_id", "card_on_dark_web")
        .agg(
          count("*").as("nb_transactions"),
          round(sum("amount_numeric"), 2).as("montant_total")
        )
        .orderBy(desc("nb_transactions"))
        .show(10)
    } else {
      println("   ✅ Aucune carte compromise (dark web) détectée")
    }

    // Taux d'erreur par client
    println("\n👤 Taux d'erreur par client (Top 10)")
    df_full.groupBy("card_client_id")
      .agg(
        count("*").as("total_trans"),
        sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).as("trans_avec_erreur")
      )
      .withColumn("taux_erreur", round(col("trans_avec_erreur") / col("total_trans") * 100, 2))
      .filter(col("total_trans") >= 100)
      .orderBy(desc("taux_erreur"))
      .show(10)

    println("💡 INDICE : Un client avec beaucoup d'erreurs est-il suspect ?")
    println("   → OUI : Taux d'erreur > 15% = tentatives de fraude répétées")
    println("   → 'Insufficient Balance' peut être normal, mais 'Bad PIN' est très suspect")

    // ==========================================================
    // PARTIE 4 – Approche fraude (sans Machine Learning)
    // ==========================================================
    println("\n" + "="*70)
    println("=== PARTIE 4 : DÉTECTION DES COMPORTEMENTS SUSPECTS ===")
    println("="*70)

    // 8. Création d'indicateurs
    println("\n8. CRÉATION D'INDICATEURS")
    
    val df_fraud_base = df_full

    val df_fraud_with_date = df_fraud_base
      .withColumn("date_only", to_date(col("Date_Parsed")))
    
    println("\n📌 INDICATEUR 1 - Transactions par carte/jour")
    val indicators_daily = df_fraud_with_date.groupBy("card_id", "date_only")
      .agg(
        count("*").as("nb_trans_jour"),
        round(sum("amount_numeric"), 2).as("montant_total_jour"),
        countDistinct("merchant_city").as("nb_villes_distinctes"),
        round(avg(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)) * 100, 2).as("ratio_erreurs_pct")
      )

    indicators_daily.orderBy(desc("nb_trans_jour")).show(5)

    println("\n📌 INDICATEURS GLOBAUX par carte")
    val indicators_global = df_fraud_base.groupBy("card_id")
      .agg(
        count("*").as("nb_total_trans"),
        round(sum("amount_numeric"), 2).as("montant_total"),
        countDistinct("merchant_city").as("nb_villes_total"),
        countDistinct(to_date(col("Date_Parsed"))).as("nb_jours_actifs")
      )

    indicators_global.orderBy(desc("nb_total_trans")).show(5)

    // 9. Détection de comportements suspects
    println("\n9. DÉTECTION DE COMPORTEMENTS SUSPECTS")
    
    val SEUIL_TRANS_JOUR = 10
    val SEUIL_VILLES = 3
    val SEUIL_MONTANT_JOUR = 1000.0

    println(s"\n🎯 Seuils utilisés :")
    println(s"   • Transactions par jour     : > $SEUIL_TRANS_JOUR")
    println(s"   • Villes différentes        : > $SEUIL_VILLES")
    println(s"   • Montant total journalier  : > $SEUIL_MONTANT_JOUR €")

    val suspicious_cards = indicators_daily.filter(
      col("nb_trans_jour") > SEUIL_TRANS_JOUR || 
      col("nb_villes_distinctes") > SEUIL_VILLES || 
      col("montant_total_jour") > SEUIL_MONTANT_JOUR
    ).withColumn("raison_suspicion",
      concat_ws(", ",
        when(col("nb_trans_jour") > SEUIL_TRANS_JOUR, 
          concat(lit("Nb trans élevé ("), col("nb_trans_jour"), lit(")"))),
        when(col("nb_villes_distinctes") > SEUIL_VILLES, 
          concat(lit("Multi-villes ("), col("nb_villes_distinctes"), lit(")"))),
        when(col("montant_total_jour") > SEUIL_MONTANT_JOUR, 
          concat(lit("Montant élevé ("), col("montant_total_jour"), lit("€)")))
      )
    )

    println(s"\n🚨 Nombre de comportements journaliers suspects détectés : ${suspicious_cards.count()}")
    println("\n📋 Top 20 comportements suspects")
    suspicious_cards.select("card_id", "date_only", "nb_trans_jour", "montant_total_jour", 
      "nb_villes_distinctes", "raison_suspicion")
      .orderBy(desc("nb_trans_jour"))
      .show(20, truncate = false)

    val suspicious_cards_summary = suspicious_cards
      .groupBy("card_id")
      .agg(
        count("*").as("nb_jours_suspects"),
        collect_set("raison_suspicion").as("raisons_multiples")
      )
      .orderBy(desc("nb_jours_suspects"))

    val unique_suspicious_cards = suspicious_cards_summary.count()
    println(s"\n→ Nombre de cartes distinctes suspectes : $unique_suspicious_cards")
    println("\n📊 Répartition des cartes suspectes :")
    suspicious_cards_summary.show(10, truncate = false)

    // ==========================================================
    // BONUS (optionnel)
    // ==========================================================
    println("\n" + "="*70)
    println("=== BONUS : SCORE DE RISQUE ===")
    println("="*70)

    println("\n🎯 FORMULE DU SCORE DE RISQUE :")
    println("   score = (nb_trans_jour × 2) + (nb_villes × 5) + (nb_erreurs × 10)")
    println("         + (nb_trans_nuit × 3) + (montant_jour > 1000€ ? 15 : 0)")
    println("\nJustification des poids :")
    println("   • nb_trans_jour (×2)  : Impact modéré, beaucoup de tx peut être normal")
    println("   • nb_villes (×5)      : Impact élevé, multi-villes = anormal")
    println("   • ratio_erreurs (×10) : Impact très élevé, erreurs = fraude potentielle")
    println("   • montant > 1000€ (+15) : Bonus pour gros montants (risque financier)")

    val df_risk_score = indicators_daily
      .join(
        df_fraud_with_date.groupBy("card_id", "date_only")
          .agg(
            sum(when(col("Heure") >= 0 && col("Heure") < 6, 1).otherwise(0)).as("nb_trans_nuit"),
            sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).as("nb_erreurs")
          ),
        Seq("card_id", "date_only"),
        "left"
      )
      .withColumn("risk_score",
        (col("nb_trans_jour") * 2) +
        (col("nb_villes_distinctes") * 5) +
        (coalesce(col("nb_erreurs"), lit(0)) * 10) +
        (coalesce(col("nb_trans_nuit"), lit(0)) * 3) +
        when(col("montant_total_jour") > 1000, 15).otherwise(0)
      )

    println("\n📊 Distribution des scores de risque")
    df_risk_score
      .withColumn("tranche_score",
        when(col("risk_score") < 10, "Faible (< 10)")
          .when(col("risk_score") < 30, "Moyen (10-30)")
          .when(col("risk_score") < 50, "Élevé (30-50)")
          .otherwise("Critique (> 50)")
      )
      .groupBy("tranche_score")
      .count()
      .orderBy(
        when(col("tranche_score") === "Faible (< 10)", 1)
          .when(col("tranche_score") === "Moyen (10-30)", 2)
          .when(col("tranche_score") === "Élevé (30-50)", 3)
          .otherwise(4)
      )
      .show()

    println("\n🏆 Top 20 cartes à risque élevé (score >= 30)")
    df_risk_score.filter(col("risk_score") >= 30)
      .select("card_id", "date_only", "nb_trans_jour", "montant_total_jour", 
        "nb_villes_distinctes", "nb_erreurs", "nb_trans_nuit", "risk_score")
      .orderBy(desc("risk_score"))
      .show(20, truncate = false)

    // Sauvegarde en Parquet
    println("\n💾 SAUVEGARDE DES RÉSULTATS")
    try {
      suspicious_cards.write.mode("overwrite").parquet("output/suspicious_cards.parquet")
      println("   ✅ suspicious_cards.parquet")
      
      df_risk_score.write.mode("overwrite").parquet("output/risk_scores.parquet")
      println("   ✅ risk_scores.parquet")
      
      indicators_global.write.mode("overwrite").parquet("output/indicators_global.parquet")
      println("   ✅ indicators_global.parquet")
      
      df_full.write.mode("overwrite").parquet("output/transactions_enriched.parquet")
      println("   ✅ transactions_enriched.parquet")
      
      println("\n📁 Fichiers sauvegardés dans le dossier 'output/'")
    } catch {
      case e: Exception => println(s"⚠️  Erreur lors de la sauvegarde : ${e.getMessage}")
    }

    // Comparaison clients normaux vs suspects
    println("\n--- COMPARAISON CLIENTS NORMAUX vs SUSPECTS ---")
    val suspicious_card_ids = suspicious_cards.select("card_id").distinct()
    
    val normal_stats = indicators_global.join(suspicious_card_ids, Seq("card_id"), "left_anti")
      .join(df_cards_clean, col("card_id") === col("card_ref_id"), "left")
      .join(df_users_clean, col("card_client_id") === df_users_clean("user_id"), "left")
      .agg(
        round(avg("nb_total_trans"), 2).as("avg_trans"),
        round(avg("montant_total"), 2).as("avg_montant"),
        round(avg("nb_villes_total"), 2).as("avg_villes"),
        round(avg("credit_score"), 2).as("avg_credit_score"),
        round(avg("current_age"), 2).as("avg_age"),
        round(avg("yearly_income"), 2).as("avg_income")
      )
      .withColumn("type_client", lit("Normal"))
    
    val suspect_stats = indicators_global.join(suspicious_card_ids, Seq("card_id"), "inner")
      .join(df_cards_clean, col("card_id") === col("card_ref_id"), "left")
      .join(df_users_clean, col("card_client_id") === df_users_clean("user_id"), "left")
      .agg(
        round(avg("nb_total_trans"), 2).as("avg_trans"),
        round(avg("montant_total"), 2).as("avg_montant"),
        round(avg("nb_villes_total"), 2).as("avg_villes"),
        round(avg("credit_score"), 2).as("avg_credit_score"),
        round(avg("current_age"), 2).as("avg_age"),
        round(avg("yearly_income"), 2).as("avg_income")
      )
      .withColumn("type_client", lit("Suspect"))

    println("\n📊 PROFIL COMPARATIF :")
    normal_stats.union(suspect_stats).show(truncate = false)

    println("\n💡 INTERPRÉTATION :")
    println("   → Les clients suspects ont généralement :")
    println("     • Plus de transactions")
    println("     • Montants totaux plus élevés")
    println("     • Credit score potentiellement plus faible")
    println("     • Profil démographique différent")

    println("\n" + "="*70)
    println("=== FIN DE L'ANALYSE ===")
    println("="*70)
   
    println("\n📝 SYNTHÈSE FINALE :")
    println(s"   • $countTrans transactions analysées")
    println(s"   • $unique_suspicious_cards cartes suspectes identifiées")
    if (cartesDarkWeb > 0) {
      println(f"   • ⚠️  $cartesDarkWeb cartes compromises (dark web) toujours actives")
    }
    println(s"   • 4 fichiers Parquet générés dans output/")
    println("\n🎓 TP complété avec succès !")

    spark.stop()
  }
}