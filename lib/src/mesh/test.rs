#![allow(unused_imports)]
use log::{error, info, debug};
use std::sync::Arc;

use serde::{Serialize, Deserialize};

use crate::prelude::*;
#[cfg(all(feature = "enable_server", feature = "enable_tcp" ))]
use crate::mesh::MeshRoot;
use crate::error::*;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct TestData {
    pub data: u128,
    pub inner: DaoVec<String>,
}

#[tokio::main(flavor = "current_thread")]
#[test]
async fn test_mesh()
{
    crate::utils::bootstrap_env();

    let cfg_ate = crate::conf::tests::mock_test_config();
    let test_url = url::Url::parse("ws://localhost/").unwrap();

    // Create a root key that will protect the integrity of the chain
    let root_key = crate::crypto::PrivateSignKey::generate(KeySize::Bit256);

    // We offset the ports so that we don't need port re-use between tests
    let port_offset = fastrand::u16(..1000);
    let port_offset = port_offset * 10;

    let mut mesh_roots = Vec::new();
    let mut cfg_mesh =
    {
        let mut roots = Vec::new();
        for n in (5100+port_offset)..(5105+port_offset) {
            roots.push(MeshAddress::new(IpAddr::from_str("127.0.0.1").unwrap(), n));
        }
        let mut cfg_mesh = ConfMesh::new("localhost", roots.iter());
        cfg_mesh.wire_protocol = StreamProtocol::WebSocket;

        let mut mesh_root_joins = Vec::new();

        // Create the first cluster of mesh root nodes
        #[allow(unused_variables)]
        let mut index: i32 = 0;
        for n in (5100+port_offset)..(5105+port_offset) {
            #[cfg(feature="enable_dns")]
            let addr = MeshAddress::new(IpAddr::from_str("0.0.0.0").unwrap(), n);
            #[cfg(not(feature="enable_dns"))]
            let addr = MeshAddress::new("localhost", n);
            #[allow(unused_mut)]
            let mut cfg_ate = cfg_ate.clone();
            #[cfg(feature = "enable_local_fs")]
            {
                cfg_ate.log_path = cfg_ate.log_path.as_ref().map(|a| format!("{}/p{}", a, index));
            }
            let mut cfg_mesh = cfg_mesh.clone();
            cfg_mesh.force_listen = Some(addr.clone());

            let root_key = root_key.as_public_key();
            let join = async move {
                let server = create_server(&cfg_mesh).await?;
                server.add_route(all_ethereal_with_root_key(root_key).await, &cfg_ate).await?;
                Result::<Arc<MeshRoot>, CommsError>::Ok(server)
            };
            mesh_root_joins.push((addr, join));
            index = index + 1;
        }

        // Wait for all the servers to start
        for (addr, join) in mesh_root_joins {
            debug!("creating server on {:?}", addr);
            let join = join.await;
            mesh_roots.push(join);
        }

        cfg_mesh
    };

    debug!("create the mesh and connect to it with client 1");
    let client_a = create_temporal_client(&cfg_ate, &cfg_mesh);
    debug!("temporal client is ready");

    let chain_a = Arc::clone(&client_a).open(&test_url, &ChainKey::from("test-chain")).await.unwrap();
    debug!("connected with client 1");

    let mut session_a = AteSession::new(&cfg_ate);
    session_a.add_user_write_key(&root_key);
    
    let dao_key1;
    let dao_key2;
    {
        let mut bus_a;
        let mut bus_b;

        let dao2;
        {
            let mut dio = chain_a.dio_ext(&session_a, TransactionScope::Full).await;
            dao2 = dio.store(TestData::default()).unwrap();
            dao_key2 = dao2.key().clone();
            let _ = dio.store(TestData::default()).unwrap();
            dio.commit().await.unwrap();

            bus_b = dao2.bus(&chain_a, dao2.inner).await;
        }

        {
            cfg_mesh.force_listen = None;
            cfg_mesh.force_client_only = true;
            let client_b = create_temporal_client(&cfg_ate, &cfg_mesh);

            let chain_b = client_b.open(&test_url, &ChainKey::new("test-chain".to_string())).await.unwrap();
            let mut session_b = AteSession::new(&cfg_ate);
            session_b.add_user_write_key(&root_key);

            bus_a = dao2.bus(&chain_b, dao2.inner).await;
            
            {
                debug!("start a DIO session for client B");
                let mut dio = chain_b.dio_ext(&session_b, TransactionScope::Full).await;

                debug!("store data object 1");
                dao_key1 = dio.store(TestData::default()).unwrap().key().clone();
                dio.commit().await.unwrap();

                debug!("load data object 2");
                let mut dao2: Dao<TestData> = dio.load(&dao_key2).await.expect("An earlier saved object should have loaded");
                
                debug!("add to new sub objects to the vector");
                dao2.push_store(&mut dio, dao2.inner, "test_string1".to_string()).unwrap();
                dio.commit().await.unwrap();
                dao2.push_store(&mut dio, dao2.inner, "test_string2".to_string()).unwrap();
                dio.commit().await.unwrap();
            }
        }

        debug!("sync to disk");
        chain_a.sync().await.unwrap();
        
        debug!("wait for an event on the BUS (local)");
        let task_ret = bus_a.recv(&session_a).await.expect("Should have received the result on the BUS");
        assert_eq!(*task_ret, "test_string1".to_string());

        debug!("wait for an event on the BUS (other)");
        let task_ret = bus_b.recv(&session_a).await.expect("Should have received the result on the BUS");
        assert_eq!(*task_ret, "test_string1".to_string());

        {
            debug!("new DIO session for client A");
            let mut dio = chain_a.dio_ext(&session_a, TransactionScope::Full).await;

            debug!("processing the next event in the BUS (and lock_for_delete it)");
            let mut task_ret = bus_b.process(&mut dio)
                .await
                .expect("Should have received the result on the BUS for the second time");
            debug!("event received");
            assert_eq!(*task_ret, "test_string2".to_string());

            // Committing the DIO
            task_ret.commit(&mut dio).unwrap();

            debug!("loading data object 1");
            dio.load::<TestData>(&dao_key1).await.expect("The data did not not get replicated to other clients in realtime");
            
            debug!("committing the DIO");
            dio.commit().await.unwrap();
        }
    }

    {
        // -DISABLED- need to implement this in the future!
        //
        // Find an address where the chain is 'not' owned which will mean the
        // server needs to do a cross connect in order to pass this test\
        // (this is needed for the WebAssembly model as this can not support
        //  client side load-balancing)
        //cfg_mesh.force_connect = cfg_mesh.roots.iter().filter(|a| Some(*a) != chain_a.remote_addr()).map(|a| a.clone()).next();
        
        cfg_mesh.force_listen = None;
        cfg_mesh.force_client_only = true;
        let client = create_temporal_client(&cfg_ate, &cfg_mesh);

        debug!("reconnecting the client");
        let chain = client.open(&test_url, &ChainKey::from("test-chain")).await.unwrap();
        let session = AteSession::new(&cfg_ate);
        {
            debug!("loading data object 1");
            let mut dio = chain.dio(&session).await;
            dio.load::<TestData>(&dao_key1).await.expect("The data did not survive between new sessions");
        }
    }

    debug!("shutting down");
    //std::process::exit(0);
}