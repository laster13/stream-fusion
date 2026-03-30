# StreamFusion V3.0.0 — Notes de version

> ⚠️ **Version majeure — Changements non rétrocompatibles**
> Une mise à jour des variables d'environnement et une migration de base de données sont **obligatoires** avant de démarrer cette version.

---

## Introduction

Après deux ans sans release majeure, StreamFusion revient avec une version 3.0.0 profondément remaniée. Cette version est le fruit d'un travail collectif intense : refonte complète de l'interface d'administration, nouvelle infrastructure de cache debrid multi-couches (Redis + PostgreSQL), vérification de cache AllDebrid en temps réel, partage de cache entre instances, support de quatre nouveaux indexeurs privés français, sécurité renforcée sur tous les endpoints, et de nombreuses améliorations de performance.

Il s'agit d'une version **breaking** : des changements de configuration et une migration de base de données sont requis.

---

## ⚠️ Breaking Changes — Migration V2 → V3

### Checklist de migration (dans l'ordre)

- [ ] Sauvegarder la base de données PostgreSQL
- [ ] Mettre à jour l'image Docker vers `v3.0.0`
- [ ] Supprimer les variables d'environnement obsolètes (voir ci-dessous)
- [ ] Mettre à jour les variables modifiées
- [ ] Ajouter les nouvelles variables si nécessaire
- [ ] Lancer les migrations BDD : `alembic upgrade head`
- [ ] Vérifier le démarrage et l'interface admin

---

### Variables supprimées — à retirer de votre configuration

| Variable | Raison |
|---|---|
| `SHAREWOOD_ENABLE` | Sharewood définitivement supprimé |
| `SHAREWOOD_URL` | idem |
| `SHAREWOOD_PASSKEY` | idem |
| `SHAREWOOD_MAX_WORKERS` | idem |
| `SHAREWOOD_UNIQUE_ACCOUNT` | idem |
| `PUBLIC_CACHE_URL` | Le cache public tiers a été supprimé |
| `ALLOW_ANONYMOUS_ACCESS` | Remplacé par `ALLOW_PUBLIC_KEY_REGISTRATION` |

---

### Variables modifiées — comportement par défaut changé

| Variable | Ancienne valeur par défaut | Nouvelle valeur par défaut | Action si usage local/Docker |
|---|---|---|---|
| `ZILEAN_HOST` | `zilean` | `zileanfortheweebs.midnightignite.me` | Ajouter `ZILEAN_HOST=zilean` |
| `ZILEAN_SCHEMA` | `http` | `https` | Ajouter `ZILEAN_SCHEMA=http` |
| `ZILEAN_PORT` | `8181` (implicite) | `None` (auto) | Ajouter `ZILEAN_PORT=8181` si nécessaire |

---

### Migrations de base de données obligatoires

Quatre nouvelles tables sont créées automatiquement :

| Table | Description |
|---|---|
| `debrid_cache` | Cache L2 PostgreSQL pour la disponibilité debrid (persistant entre redémarrages) |
| `peer_keys` | Clés d'authentification pour le partage inter-instances |
| `metadata_mappings` | Correspondances IMDb/TMDB personnalisées via l'interface admin |

```bash
alembic upgrade head
```

---

### Suppression de Sharewood

Le tracker **Sharewood** a été **entièrement supprimé** de la codebase (API, service, modèles). Si vous utilisiez Sharewood, il n'existe pas d'équivalent direct — consultez les nouveaux indexeurs supportés ci-dessous.

---

## Nouveautés

### Quatre nouveaux indexeurs privés

Quatre trackers UNIT3D et Torznab rejoignent StreamFusion :

| Indexeur | URL | Variables à configurer |
|---|---|---|
| **ABN** (Abnormal) | `abn.lol` | `ABN_API_KEY`, `ABN_PASSKEY` |
| **G3MINI** (Gemini Tracker) | `gemini-tracker.org` | `G3MINI_API_KEY`, `G3MINI_PASSKEY` |
| **TheOldSchool** | `theoldschool.cc` | `THEOLDSCHOOL_API_KEY`, `THEOLDSCHOOL_PASSKEY` |
| **Generation Free** | `generation-free.org` | `GENERATIONFREE_API_KEY`, `GENERATIONFREE_PASSKEY` |

Ces indexeurs sont activés par défaut (`_ENABLE=true`). Ils n'interrogent le serveur que si des credentials sont fournis, ou si `*_UNIQUE_ACCOUNT` est configuré côté serveur.

---

### Infrastructure de cache debrid entièrement repensée

**Cache Redis partagé entre tous les providers**
Tous les résultats de disponibilité debrid (RealDebrid, AllDebrid, TorBox…) sont désormais mis en cache dans Redis avec un TTL de 7 jours (fichiers disponibles) ou 1 heure (fichiers non disponibles). Les vérifications redondantes sont évitées à l'échelle de l'instance.

**Cache L2 PostgreSQL persistant**
Un second niveau de cache est maintenu en base de données. Les informations de disponibilité survivent aux redémarrages de l'application, ce qui accélère considérablement les premières requêtes après un restart.

**Vérification AllDebrid en temps réel (bulk upload/delete)**
AllDebrid dispose désormais d'une vérification de cache réelle via upload en masse et suppression immédiate des magnets temporaires. Les faux positifs sont éliminés, les résultats sont fiables. Les magnets invalides (`MAGNET_INVALID_URI`) sont mis en cache comme non-disponibles (TTL 1h) pour éviter les re-tentatives inutiles.

**RealDebrid via API directe**
La vérification de disponibilité RealDebrid passe désormais par l'API RD directe, sans intermédiaire StremThru (le fallback reste disponible si StremThru est configuré).

---

### Partage de cache inter-instances (Peer-to-Peer)

Deux instances StreamFusion peuvent désormais partager leur cache debrid de façon sécurisée. L'instance "peer" expose ses données de disponibilité via une clé d'authentification dédiée.

Nouvelles variables d'environnement (optionnelles) :

```env
PEER_STREAMFUSION_URL=https://votre-instance-peer.exemple.com
PEER_STREAMFUSION_KEY_ID=<key_id fourni par l'instance peer>
PEER_STREAMFUSION_SECRET=<secret 256-bit hex fourni par l'instance peer>
```

La gestion des clés peer se fait depuis le nouvel **interface d'administration**.

---

### Interface d'administration entièrement redessinée

L'ancienne interface admin a été remplacée par une UI complète en Jinja2 :

- **Dashboard** — Vue d'ensemble de l'instance
- **Clés API** — Gestion des utilisateurs et de leurs clés d'accès
- **Clés Peer** — Gestion des autorisations inter-instances
- **Mappings metadata** — Correspondances IMDb/TMDB manuelles (pour corriger des métadonnées incorrectes)
- **Recherche par hash** — Vérification directe de disponibilité debrid
- **Recherche TMDB** — Recherche et visualisation de métadonnées
- **Maintenance** — Actions de maintenance et nettoyage
- **Configuration** — Page de config avec encodage en temps réel

---

### Page d'inscription publique

Une page `/register` permet aux utilisateurs de générer eux-mêmes leur clé API, sans intervention de l'administrateur.

- Désactivée par défaut
- Rate limiting Redis intégré contre les abus
- Templates Jinja2 (page active + page désactivée)

```env
ALLOW_PUBLIC_KEY_REGISTRATION=true
```

---

### Sécurité

**Chiffrement Fernet des tokens de configuration**
Les tokens de configuration encodés dans les URLs d'addon peuvent désormais être chiffrés avec une clé Fernet. Sans cette clé, le fallback est l'encodage Base64 (comportement V2).

```env
CONFIG_SECRET_KEY=<clé Fernet générée via Fernet.generate_key()>
```

> ⚠️ Changer cette clé invalide toutes les URLs d'addon existantes.

**Protection CSRF**
L'endpoint `POST /api/config/encode` est protégé contre les attaques CSRF.

**Rate limiting sur la lecture**
Les requêtes de playback sont limitées par utilisateur pour prévenir les abus :

```env
PLAYBACK_LIMIT_REQUESTS=60   # requêtes max par fenêtre (défaut : 60)
PLAYBACK_LIMIT_SECONDS=60    # taille de la fenêtre en secondes (défaut : 60)
```

**Clés de cache hashées**
Les clés de cache Redis pour le playback sont désormais hashées (plus d'informations sensibles en clair dans Redis).

**Avertissement session key**
Au démarrage, un warning est émis si `SESSION_KEY` utilise la valeur par défaut non sécurisée. Pensez à définir une valeur unique :

```env
SESSION_KEY=<chaîne aléatoire de 64 caractères>
```

**Redaction des secrets dans les logs**
Tous les tokens, clés API et secrets sont automatiquement masqués dans les logs.

---

### Contrôle des téléchargements debrid

Nouvelle variable pour les instances publiques — masque les streams non-cachés (téléchargements debrid directs) dans les résultats Stremio :

```env
ALLOW_DEBRID_DOWNLOAD=false
```

Par défaut à `true` (comportement V2 conservé).

---

### StremThru SDK

L'intégration StremThru repose désormais sur le SDK officiel. L'ancien fichier `stremthru.py` custom a été remplacé. L'URL par défaut est `https://stremthru.13377001.xyz` (configurable via `STREMTHRU_URL`).

---

### Recherche & Filtrage

- **Phases de recherche configurables** par indexeur (primaire, secondaire, fallback)
- **Optimisation RealDebrid** dans le flux de recherche
- **Préférence TorBox** pour les indexeurs français (configurable)
- **Amélioration du service C411** — meilleure gestion des logs et des requêtes
- **Refactoring asyncio** avec `asyncio.to_thread` pour les opérations de filtrage (non-bloquant)
- **Parallelisation** de certaines opérations de cache et de recherche

---

### Logs & Observabilité

- Nouveau niveau `TRACE` pour les logs très verbeux (en dessous de DEBUG)
- Logs info dégradés en debug/trace — moins de bruit en production
- Meilleure redaction des secrets dans tous les appels sortants

---

### TMDB

Nouvelle variable pour forcer la langue des métadonnées TMDB :

```env
TMDB_LANGUAGE=fr-FR   # défaut
```

---

## Récapitulatif des nouvelles variables d'environnement

| Variable | Défaut | Description |
|---|---|---|
| `CONFIG_SECRET_KEY` | `None` | Clé Fernet pour chiffrer les tokens de config dans les URLs |
| `ALLOW_DEBRID_DOWNLOAD` | `true` | `false` pour masquer les streams non-cachés (instances publiques) |
| `ALLOW_PUBLIC_KEY_REGISTRATION` | `false` | Activer la page d'inscription publique `/register` |
| `PLAYBACK_LIMIT_REQUESTS` | `60` | Requêtes max par utilisateur par fenêtre de rate limit |
| `PLAYBACK_LIMIT_SECONDS` | `60` | Taille de la fenêtre de rate limit (secondes) |
| `TMDB_LANGUAGE` | `fr-FR` | Langue pour les appels TMDB |
| `PEER_STREAMFUSION_URL` | `""` | URL de l'instance peer pour le partage de cache |
| `PEER_STREAMFUSION_KEY_ID` | `""` | Key ID émise par l'instance peer |
| `PEER_STREAMFUSION_SECRET` | `""` | Secret 256-bit hex de l'instance peer |
| `GENERATIONFREE_API_KEY` | `None` | Clé API Generation Free |
| `GENERATIONFREE_PASSKEY` | `None` | Passkey Generation Free |
| `ABN_API_KEY` | `None` | Clé API ABN (Abnormal) |
| `ABN_PASSKEY` | `None` | Passkey ABN |
| `G3MINI_API_KEY` | `None` | Clé API G3MINI (Gemini Tracker) |
| `G3MINI_PASSKEY` | `None` | Passkey G3MINI |
| `THEOLDSCHOOL_API_KEY` | `None` | Clé API TheOldSchool |
| `THEOLDSCHOOL_PASSKEY` | `None` | Passkey TheOldSchool |

---

## Contributeurs

Un grand merci à toutes les personnes qui ont rendu cette version possible :

- **[LimeDrive](https://github.com/LimeDrive)** — Créateur du projet et développeur principal de cette V3 : architecture, sécurité, infrastructure debrid, interface admin, indexeurs, performance
- **[HyPnoTiiK](https://github.com/HyPnoTiiK)** — Refonte de la page d'inscription (`/register`) et redesign de la page de configuration admin
- **[laster13](https://github.com/laster13)** — Améliorations de la recherche, vérification de cache AllDebrid, intégrations debrid et corrections diverses

Merci également à **Telkaoss** et **Elfbot** (ElfHosted) pour leurs contributions passées qui ont servi de base à cette version.

Et bien sûr, merci à toute la communauté pour les retours, les tests et le soutien.

---

*StreamFusion est un projet open source à but éducatif, distribué sous licence MIT.*
