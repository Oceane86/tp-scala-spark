# TP Scala & Spark - Analyse de Fraude Bancaire

---

## 📋 Synthèse Finale (Partie 5)

### 🎯 Patterns Principaux Observés

#### 1. Patterns de Volume
* **Concentration des transactions** : La majorité des transactions se situent dans des tranches de montants faibles (< 50€), ce qui correspond à des achats du quotidien.
* **Heures de pointe** : Pics d'activité observés lors des pauses déjeuner et en fin d'après-midi, correspondant aux comportements d'achat classiques.
* **Jours de la semaine** : Activité plus intense les jours ouvrables (mercredi/jeudi) par rapport aux week-ends.

#### 2. Patterns de Comportement Suspect
* **Multi-localisation** : Utilisation d'une même carte dans plus de 3 villes différentes le même jour (vol de carte, partage de compte ou fraude dispersée).
* **Volume anormal** : Cartes effectuant plus de 10 transactions par jour (tests de cartes volées ou micro-transactions frauduleuses).
* **Montants élevés** : Dépassement du seuil de 1000€ journalier par carte, signalant une rupture de pattern habituel.

#### 3. Patterns d'Erreurs
* **Taux d'erreur élevé** : Les cartes dépassant 20% d'erreurs (PIN erroné, CVV faux) sont souvent liées à des tentatives de force brute.
* **Types d'erreurs** : Concentration sur les refus d'autorisation et les fonds insuffisants répétés.

#### 4. Patterns de Catégories MCC
* **Secteurs à risque** : Électronique, Bijouterie et Stations-service (biens facilement revendables).
* **Incohérence Profil/Achat** : Transactions dans des catégories jamais explorées par le client auparavant.

---

### 📊 Indicateurs pour le Machine Learning

#### **Indicateurs Comportementaux** ⭐
1.  **Volume journalier** : Nombre de transactions par 24h.
2.  **Dispersion géographique** : Nombre de villes distinctes visitées.
3.  **Ratio d'erreurs** : Pourcentage de transactions échouées.
4.  **Variabilité des montants** : Écart-type des dépenses habituelles.

#### **Indicateurs Temporels & Relationnels**
5.  **Horaires atypiques** : Activité nocturne (2h-6h du matin).
6.  **Fréquence d'utilisation** : Jours consécutifs d'activité.
7.  **Score Marchand** : Risque historique associé au merchant_id.

#### **Score de Risque Composite** 🎯
* **0-3** : Risque faible (Vert)
* **4-6** : Risque moyen (Orange) - Alerte générée
* **7+** : Risque élevé (Rouge) - Blocage automatique

---

### ⚠️ Limites des Données

* **Qualité** : Valeurs manquantes sur les MCC et erreurs textuelles non structurées.
* **Contexte** : Absence de "labels" (on ne sait pas quelle transaction est une fraude réelle).
* **Géographie** : Précision limitée à la ville (pas de coordonnées GPS ou de pays).
* **Technique** : Pas de "Device Fingerprinting" (IP, type de téléphone, etc.).

---

### 🚀 Recommandations

1.  **Court Terme** : Nettoyer les montants incohérents (≤ 0) et enrichir l'historique client.
2.  **Moyen Terme** : Implémenter un modèle supervisé (XGBoost ou Random Forest) une fois les labels obtenus.
3.  **Long Terme** : Déployer une détection en temps réel via **Spark Streaming**.

---

### 🔧 Utilisation du Code

```bash
# S'assurer d'être à la racine du projet
# Exécuter l'analyse complète avec scala-cli
scala-cli run exoTP.scala

# Vérifier les dossiers de sortie
ls output/
