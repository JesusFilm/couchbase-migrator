# Couchbase Migrator

## Getting Started

Once the container is built, use the Node version declared in `.nvmrc`:

```bash
nvm use
```

## Runtime and native SDK review

The packaged help/version paths do not connect to databases or providers.
Generate clients with `pnpm prisma:generate`, then run `pnpm build`.

The repository currently locks Couchbase SDK 3.2.7. The vendor's Node.js
[compatibility matrix](https://docs.couchbase.com/nodejs-sdk/current/project-docs/compatibility.html)
lists Node 24 support from SDK 4.7.0. Compiling or importing SDK 3.2.7 on Node 24
is local compatibility evidence only; it does not establish vendor support.

A maintained runtime proposal is Node 24 with a separately reviewed Couchbase
4.7.x SDK migration. That major SDK change needs review of the native backend,
existing SDK calls and target server compatibility before operational acceptance.
No SDK major upgrade is included here. Help/version smoke tests and offline unit
tests do not establish migration, provider or database behavior.

## Environment Setup

Before running the script, you need to set up the database URLs in your environment variables. The following environment variables are required:

- `PRISMA_API_USERS_URL` - Database connection URL for API Users
- `PRISMA_API_MEDIA_URL` - Database connection URL for API Media
- `PRISMA_USERS_URL` - Database connection URL for Users (optional), this is a SQLite Database used to temporarily store user data needed for other objects.
- `GOOGLE_APPLICATION_JSON` - Firebase database to save users
- `OKTA_TOKEN` - needed to validate user's SSO GUID against.

These should be set in your `.env` file in the project root.

## Schema Setup

**Important:** Before pulling the schema, the environment variables (`PRISMA_API_USERS_URL` and `PRISMA_API_MEDIA_URL`) must point to the **production** database to pull the schema.

Once the database URLs are configured to point to production, pull the schema using the pull command:

```bash
pnpm prisma:pull
```

**After pulling the schema:**

- If you want to run the script **locally**, change the environment variables to point to your **container database**
- If you want to run scripts against **stage or prod**, keep the environment variables pointing to the respective production databases

## Running the Script

After setting up the environment variables and pulling the schema, you can run the script. The script supports the following commands:

- `build-cache` - Build document cache by migrating documents from Couchbase
- `ingest` - Ingest documents from cache into Core

### Example usage :

#### 1. Pull the database schema (from production)

**First, ensure your `.env` file has the production database URLs:**

- `PRISMA_API_USERS_URL` → production database
- `PRISMA_API_MEDIA_URL` → production database

```bash
pnpm prisma:pull
```

#### 2. Configure environment for local development (optional)

If you want to run the script locally, update your `.env` file to point to your container database:

- `PRISMA_API_USERS_URL` → local container database
- `PRISMA_API_MEDIA_URL` → local container database

#### 3. Build the cache

```bash
pnpm dev build-cache
```

#### 4. Ingest into core from cache

```bash
pnpm dev ingest --pipeline users
pnpm dev ingest --pipeline playlists
pnpm dev ingest --pipeline all
```

**Note:** If you want to run scripts against stage or prod, ensure your environment variables point to the respective production databases in your `.env` file.

## Additional Info.

`reset:firebase` is used to batch delete firebase users. be warned this is dangerous - only do this in dev environment.

This is becuase the email could be used to login to other Jesus Film Apps, ergo, this should only be used to test scripting in dev environments.

## Offline CI

Pull requests and pushes to `main` run lint, formatting, filesystem error-reporting tests, Prisma client generation from committed schemas, and the TypeScript build. Install uses `--ignore-scripts`: these checks do not execute the Couchbase native addon. No environment file, database introspection, migrations, Firebase, or Okta access is needed.

Run `pnpm test` and `pnpm prisma:generate && pnpm build` locally. Successful CI validates offline code only; native Couchbase connectivity and end-to-end ingestion still require a separately authorized non-production integration environment.
