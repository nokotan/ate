#![cfg(any(feature = "enable_server", feature = "enable_client" ))]
use log::{info};
use serde::{Serialize, Deserialize};
use ate::prelude::*;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct MyTestObject
{
    firstname: String,
    lastname: String,
    data1: [u8; 32],
    data2: [u8; 32],
    data3: [u8; 32],
    data4: Vec<u128>,
}

#[test]
fn load_test() -> Result<(), AteError> {
    ate::utils::bootstrap_env();

    #[cfg(feature = "enable_mt")]
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    #[cfg(not(feature = "enable_mt"))]
    let rt =tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    
    rt.block_on(TaskEngine::run_until(async {
        // The default configuration will store the redo log locally in the temporary folder
        let mut conf = ConfAte::default();
        conf.configured_for(ConfiguredFor::BestPerformance);
        let builder = ChainBuilder::new(&conf).await.build();

        {
            // We create a chain with a specific key (this is used for the file name it creates)
            let chain = builder.open_local(&ChainKey::from("load")).await?;
            
            // Prepare
            let session = AteSession::new(&conf);
            let mut test_obj = MyTestObject {
                firstname: "Joe".to_string(),
                lastname: "Blogs".to_string(),
                data1: [0 as u8; 32],
                data2: [1 as u8; 32],
                data3: [2 as u8; 32],
                data4: Vec::new()
            };
            for _ in 0..100 {
                test_obj.data4.push(1234 as u128);
            }

            // Do a whole let of work
            info!("create::running");
            for _ in 0..100 {
                let mut dio = chain.dio(&session).await;
                for _ in 0..100 {
                    dio.store(test_obj.clone())?;
                }
                dio.commit().await?;
            }
            info!("create::finished");
        }

        {
            // We create a chain with a specific key (this is used for the file name it creates)
            info!("load::running");
            let chain = builder.open_local(&ChainKey::from("load")).await?;
            info!("load::finished");

            // Destroy the chain
            chain.single().await.destroy().await.unwrap();
        }

        Ok(())
    }))
}