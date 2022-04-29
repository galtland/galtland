// SPDX-License-Identifier: AGPL-3.0-only

use std::env;

use galtcore::libp2p::identity::{self, Keypair};
use sqlx::migrate::{MigrateDatabase, Migrator};
use sqlx::sqlite::SqlitePool;
use sqlx::{Pool, Sqlite};

static MIGRATOR: Migrator = sqlx::migrate!(); // defaults to "./migrations"

pub struct Db {
    pool: Pool<Sqlite>,
}

impl Db {
    pub async fn get() -> anyhow::Result<Self> {
        let pool = {
            //FIXME: create DB in HOME
            let uri = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:galt.db".to_string());
            if !sqlx::any::Any::database_exists(&uri).await? {
                sqlx::any::Any::create_database(&uri).await?;
            }
            let pool = SqlitePool::connect(&uri).await?;
            MIGRATOR.run(&pool).await?;
            pool
        };
        Ok(Self { pool })
    }

    pub async fn get_or_create_org_keypair(&mut self) -> anyhow::Result<Keypair> {
        let mut c = self.pool.acquire().await?;
        let r = sqlx::query!(
            r#"
                select keypair from keypairs
                where name = 'org'
            "#
        )
        .fetch_optional(&mut c)
        .await?;
        let k = match r {
            Some(r) => identity::Keypair::from_protobuf_encoding(&r.keypair)?,
            None => {
                let k = identity::Keypair::generate_ed25519();
                let blob = k.to_protobuf_encoding()?;
                sqlx::query!(
                    r#"insert into keypairs (keypair, name) values (?, 'org')"#,
                    blob
                )
                .execute(&mut c)
                .await?;
                k
            }
        };

        Ok(k)
    }
}
