# Couchbase Migrator

## Environment Setup

Before running the script, you need to set up the database URLs in your environment variables. The following environment variables are required:

- `PRISMA_API_USERS_URL` - Database connection URL for API Users
- `PRISMA_API_MEDIA_URL` - Database connection URL for API Media
- `PRISMA_USERS_URL` - Database connection URL for Users (optional), this is a SQLite Database used to temporarily store user data needed for other objects.
- `GOOGLE_APPLICATION_JSON` - Firebase database to save users
- `OKTA_TOKEN` - needed to validate user's SSO GUID against.

These should be set in your `.env` file in the project root.

## Schema Setup

Once the database URLs are configured, you need to pull the schema using the pull command. **This must be done before running the script.**

```bash
pnpm prisma:pull
```

## Running the Script

After setting up the environment variables and pulling the schema, you can run the script. The script supports the following commands:

- `build-cache` - Build document cache by migrating documents from Couchbase
- `ingest` - Ingest documents from cache into Core

###Example usage :

#### 1. pull the database schema

```bash
pnpm prisma:pull
```

#### 2. build the cache

```bash
pnpm dev build-cache

```

#### 3. ingest into core from cache.

```bash
pnpm dev ingest --pipeline users
pnpm dev ingest --pipeline playlists
pnpm dev ingest --pipeline all
```

## Additional Info.

`reset:firebase` is used to batch delete firebase users. be warned this is dangerous - only do this in dev environment.

This is becuase the email could be used to login to other Jesus Film Apps, ergo, this should only be used to test scripting in dev environments.
