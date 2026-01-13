# TP Scala & Spark - Analyse de Fraude Bancaire

## 📋 Synthèse Finale (Partie 5)

### 🎯 Patterns Principaux Observés

#### 1. **Patterns de Volume**
- **Concentration des transactions** : La majorité des transactions se situent dans des tranches de montants faibles (< 50€), ce qui correspond à des achats du quotidien
- **Heures de pointe** : On observe des pics d'activité à des heures précises (midi, fin d'après-midi), ce qui correspond aux comportements d'achat normaux
- **Jours de la semaine** : Les jours ouvrables génèrent plus de transactions que les week-ends, avec une concentration sur les mercredis et jeudis

#### 2. **Patterns de Comportement Suspect**
- **Multi-localisation** : Certaines cartes effectuent des transactions dans plusieurs villes différentes le même jour (> 3 villes), ce qui peut indiquer :
  - Une carte volée utilisée par plusieurs personnes
  - Des achats en ligne avec des adresses de livraison multiples
  - Des transactions frauduleuses géographiquement dispersées

- **Volume anormal** : Cartes avec plus de 10 transactions par jour sortent de la norme statistique et peuvent indiquer :
  - Des tests de validation de carte volée (micro-transactions)
  - Une utilisation intensive non légitime
  - Des achats fractionnés pour passer sous les radars

- **Montants élevés concentrés** : Montants journaliers dépassant 1000€ sur une seule carte, surtout si inhabituel pour ce client

#### 3. **Patterns d'Erreurs**
- **Erreurs répétées** : Les cartes avec des taux d'erreur élevés (> 20%) peuvent indiquer :
  - Des tentatives de transactions avec des informations incorrectes (PIN, CVV)
  - Des problèmes techniques récurrents
  - Des tentatives frauduleuses multiples

- **Types d'erreurs spécifiques** : Certains types d'erreurs sont plus révélateurs que d'autres (refus d'autorisation, fonds insuffisants répétés)

#### 4. **Patterns de Catégories MCC**
- **Catégories à risque** : Certaines catégories de marchands présentent des montants moyens plus élevés et sont plus ciblées par la fraude :
  - Électronique (facilement revendable)
  - Bijouterie et métaux précieux
  - Stations-service (carburant revendable)
  
- **Catégories anormales** : Des transactions dans des catégories inhabituelles pour un profil client donné

---

### 📊 Indicateurs Utiles pour un Futur Modèle de Machine Learning

#### **Indicateurs Comportementaux** ⭐
1. **Nombre de transactions par jour** : Détecte les volumes anormaux
2. **Nombre de villes distinctes par période** : Identifie la dispersion géographique
3. **Ratio d'erreurs** : Indicateur de tentatives frauduleuses
4. **Variabilité des montants** : Écart-type des montants pour détecter les changements de pattern

#### **Indicateurs Temporels**
5. **Transactions hors horaires habituels** : Nuit (2h-6h), jours fériés
6. **Écart par rapport au pattern habituel du client** : Déviation par rapport à la moyenne personnelle
7. **Fréquence d'utilisation** : Nombre de jours consécutifs d'activité

#### **Indicateurs Transactionnels**
8. **Montant moyen par transaction** : Comparé à l'historique du client
9. **Montant total par période** : Détection de pics inhabituels
10. **Catégories MCC inhabituelles** : Transactions dans des catégories jamais utilisées avant

#### **Indicateurs Relationnels**
11. **Nombre de marchands distincts** : Diversité anormale
12. **Transactions répétées chez le même marchand** : Peut indiquer des tests
13. **Score de risque du marchand** : Basé sur l'historique de fraudes

#### **Indicateurs Géographiques**
14. **Distance entre transactions successives** : Déplacements impossibles (100km en 10 min)
15. **Transactions dans des pays à risque** : Liste noire géographique
16. **Changement soudain de zone géographique** : Passage d'une ville à une autre sans transition

#### **Score de Risque Composite** 🎯
Un score combinant tous ces indicateurs pondérés permettrait de classifier les transactions en temps réel :
- **0-3** : Risque faible (vert)
- **4-6** : Risque moyen (orange) - Alerte
- **7+** : Risque élevé (rouge) - Blocage manuel

---

### ⚠️ Limites des Données

#### **1. Limites de Qualité**
- **Valeurs manquantes** : Certaines colonnes présentent des taux élevés de données manquantes (MCC, erreurs), ce qui réduit la précision des analyses
- **Données incohérentes** : Présence de montants ≤ 0, transactions sans date valide
- **Encodage des erreurs** : Format texte non structuré (CSV séparé par virgules) complique l'analyse fine

#### **2. Limites de Contexte**
- **Absence d'historique client** : Impossible de déterminer le comportement "normal" de chaque client sur une longue période
- **Pas de labels de fraude confirmée** : On ne sait pas quelles transactions sont réellement frauduleuses, ce qui empêche un apprentissage supervisé
- **Pas d'informations sur le marchand** : Manque de données sur la réputation, la catégorie réelle, l'historique de fraude des marchands

#### **3. Limites Temporelles**
- **Période limitée** : Les données couvrent probablement une courte période, insuffisante pour détecter les tendances saisonnières
- **Pas de granularité fine** : L'heure est disponible mais pas les secondes, ce qui limite la détection de séquences rapides
- **Absence de timestamps précis** : Difficile de reconstituer l'ordre exact des transactions

#### **4. Limites Géographiques**
- **Ville uniquement** : Pas de code postal, coordonnées GPS, ou pays pour une analyse géographique fine
- **Pas de distance calculée** : Impossible de mesurer précisément les déplacements entre transactions

#### **5. Limites Techniques**
- **Absence de features calculées** : Pas de vitesse de frappe, IP, device fingerprint, qui sont cruciales en détection de fraude moderne
- **Pas de données biométriques** : Authentification par PIN uniquement
- **Pas d'informations réseau** : Canal de transaction (online, POS, ATM) non spécifié clairement

#### **6. Limites Métier**
- **Définition de "suspect" arbitraire** : Les seuils (10 trans/jour, 3 villes, 1000€) sont définis sans base statistique solide
- **Faux positifs potentiels** : Un client en déplacement professionnel peut déclencher les alertes sans être frauduleux
- **Pas de coût-bénéfice** : On ne connaît pas le coût d'un faux positif vs le coût d'une fraude non détectée

---

### 🚀 Recommandations pour Amélioration

#### **Court Terme**
1. **Enrichir les données** avec l'historique complet des clients (6-12 mois)
2. **Obtenir les labels** : Identifier les transactions réellement frauduleuses
3. **Ajouter des features externes** : Jours fériés, événements locaux, météo

#### **Moyen Terme**
4. **Créer des profils clients** : Comportement moyen, préférences, patterns
5. **Implémenter un modèle supervisé** : Random Forest, XGBoost, ou réseaux de neurones
6. **Mettre en place une détection en temps réel** : API Spark Streaming

#### **Long Terme**
7. **Intégrer des données externes** : Bureau de crédit, listes noires, réputation marchands
8. **Utiliser du Deep Learning** : LSTM pour séquences temporelles, Autoencodeurs pour anomalies
9. **Feedback loop** : Réentraîner le modèle avec les retours des analystes fraude

---

### 📈 Conclusion

Cette analyse exploratoire a permis de :
- ✅ Identifier des patterns suspects clairs (multi-localisation, volumes anormaux)
- ✅ Créer des indicateurs quantitatifs exploitables
- ✅ Proposer une base solide pour un futur modèle de ML

**Prochaine étape critique** : Obtenir des labels de fraude confirmée pour passer d'une approche descriptive (règles) à une approche prédictive (Machine Learning).

---

### 🔧 Utilisation du Code

```bash
# Exécuter l'analyse complète
scala-cli run exoTP.scala

# Résultats attendus dans output/
# - suspicious_cards.parquet : Comportements suspects
# - risk_scores.parquet : Scores de risque
# - indicators_global.parquet : Indicateurs par carte
```

---

**Date** : Janvier 2026  
**Framework** : Apache Spark 3.5.0 / Scala 2.13#   t p - s c a l a - s p a r k  
 