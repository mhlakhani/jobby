use crate::{db::JobbyDb, Jobby, SqliteJobbyStore};

use std::sync::Arc;

use rocket::fairing::AdHoc;

pub fn stage() -> AdHoc {
    AdHoc::on_ignite("setup jobby db", |rocket| async {
        let rocket = rocket.attach(JobbyDb::fairing());
        // Need the two stage workaround so the DB is ready by the time this runs
        rocket.attach(AdHoc::try_on_ignite("create jobby", |rocket| async {
            let pool = JobbyDb::pool(&rocket);
            let Some(pool) = pool else {
                println!("Error creating jobby: no jobby pool!");
                return Err(rocket);
            };
            let store = Arc::new(SqliteJobbyStore(pool.clone()));
            match Jobby::new(&rocket, store).await {
                Ok(jobby) => Ok(rocket.manage(jobby).attach(AdHoc::on_shutdown(
                    "jobby shutdown",
                    |rocket| {
                        Box::pin(async move {
                            if let Some(jobby) = rocket.state::<Jobby>() {
                                jobby.shutdown();
                            }
                        })
                    },
                ))),
                Err(e) => {
                    println!("Error creating jobby: {e:?}");
                    Err(rocket)
                }
            }
        }))
    })
}
