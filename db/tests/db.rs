use bonerjams_config::database::DbOpts;
use bonerjams_db::prelude::*;
use bonerjams_db::Database;
use bonerjams_db::DbBatch;
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
pub struct TestData {
    pub key: String,
    pub foo: String,
}

impl DbKey for TestData {
    fn key(&self) -> anyhow::Result<Vec<u8>> {
        Ok(self.key.as_bytes().to_vec())
    }
}

#[test]
fn test_db_basic() {
    let db_opts = DbOpts {
        path: "test_db_basic.db".to_string(),
        ..Default::default()
    };

    let db = Database::new(&db_opts).unwrap();
    let insert = || {
        let mut db_batch = DbBatch::new();
        db_batch
            .insert(&TestData {
                key: "key1".to_string(),
                foo: "foo1".to_string(),
            })
            .unwrap();
        {
            let tree = db.open_tree(DbTrees::Custom("foobar")).unwrap();
            tree.apply_batch(&mut db_batch).unwrap();
            tree.flush().unwrap();
            assert_eq!(tree.len(), 1);
        }

        db_batch
            .insert(&TestData {
                key: "key2".to_string(),
                foo: "foo2".to_string(),
            })
            .unwrap();
        {
            let tree = db.open_tree(DbTrees::Custom("foobar")).unwrap();
            tree.apply_batch(&mut db_batch).unwrap();
            tree.flush().unwrap();
            assert_eq!(tree.len(), 2);
        }

        db_batch
            .insert(&TestData {
                key: "key3".to_string(),
                foo: "foo3".to_string(),
            })
            .unwrap();
        {
            let tree = db.open_tree(DbTrees::Custom("foobarbaz")).unwrap();
            tree.apply_batch(&mut db_batch).unwrap();
            tree.flush().unwrap();
            assert_eq!(tree.len(), 1);
        }
        db_batch
            .insert(&TestData {
                key: "key4".to_string(),
                foo: "foo4".to_string(),
            })
            .unwrap();
        {
            db.apply_batch(&mut db_batch).unwrap();
        }
        db_batch
            .insert_raw("rawkey".as_bytes(), "rawvalue".as_bytes())
            .unwrap();
        {
            let tree = db.open_tree(DbTrees::Binary("rawkeys".as_bytes())).unwrap();
            tree.apply_batch(&mut db_batch).unwrap();
            tree.flush().unwrap();
            assert_eq!(tree.len(), 1);
        }
        db.flush().unwrap();
    };
    let query = || {
        let foobar_values = db.list_values(DbTrees::Custom("foobar")).unwrap();
        assert_eq!(foobar_values.len(), 2);
        let test_data_one: TestData = db
            .open_tree(DbTrees::Custom("foobar"))
            .unwrap()
            .deserialize(foobar_values[0].0.clone())
            .unwrap();
        assert_eq!(test_data_one.key, "key1".to_string());
        assert_eq!(test_data_one.foo, "foo1".to_string());
        let test_data_two: TestData = db
            .open_tree(DbTrees::Custom("foobar"))
            .unwrap()
            .deserialize(foobar_values[1].0.clone())
            .unwrap();
        assert_eq!(test_data_two.key, "key2".to_string());
        assert_eq!(test_data_two.foo, "foo2".to_string());
        let foobarbaz_values = db.list_values(DbTrees::Custom("foobarbaz")).unwrap();
        assert_eq!(foobarbaz_values.len(), 1);
        let test_data_three: TestData = db
            .open_tree(DbTrees::Custom("foobarbaz"))
            .unwrap()
            .deserialize(foobarbaz_values[0].0.clone())
            .unwrap();
        assert_eq!(test_data_three.key, "key3".to_string());
        assert_eq!(test_data_three.foo, "foo3".to_string());
        let default_tree_values = db.list_values(DbTrees::Default).unwrap();
        assert_eq!(default_tree_values.len(), 1);
        let raw_values = db
            .list_values(DbTrees::Binary("rawkeys".as_bytes()))
            .unwrap();
        assert_eq!(raw_values.len(), 1);
        assert_eq!(raw_values[0].0, "rawkey".as_bytes());
        assert_eq!(raw_values[0].1, "rawvalue".as_bytes());
    };
    insert();
    query();
    db.destroy();
    std::fs::remove_dir_all("test_db_basic.db").unwrap();
}

// same as test_db_basic, but uses the async flush functions instead
#[tokio::test]
async fn test_db_basic_async() {
    let db_opts = DbOpts {
        path: "test_db_basic_async.db".to_string(),
        ..Default::default()
    };

    let db = Database::new(&db_opts).unwrap();
    async {
        let mut db_batch = DbBatch::new();
        db_batch
            .insert(&TestData {
                key: "key1".to_string(),
                foo: "foo1".to_string(),
            })
            .unwrap();
        {
            let tree = db.open_tree(DbTrees::Custom("foobar")).unwrap();
            tree.apply_batch(&mut db_batch).unwrap();
            tree.flush_async().await.unwrap();
            assert_eq!(tree.len(), 1);
        }

        db_batch
            .insert(&TestData {
                key: "key2".to_string(),
                foo: "foo2".to_string(),
            })
            .unwrap();
        {
            let tree = db.open_tree(DbTrees::Custom("foobar")).unwrap();
            tree.apply_batch(&mut db_batch).unwrap();
            tree.flush_async().await.unwrap();
            assert_eq!(tree.len(), 2);
        }

        db_batch
            .insert(&TestData {
                key: "key3".to_string(),
                foo: "foo3".to_string(),
            })
            .unwrap();
        {
            let tree = db.open_tree(DbTrees::Custom("foobarbaz")).unwrap();
            tree.apply_batch(&mut db_batch).unwrap();
            tree.flush_async().await.unwrap();
            assert_eq!(tree.len(), 1);
        }
        db_batch
            .insert(&TestData {
                key: "key4".to_string(),
                foo: "foo4".to_string(),
            })
            .unwrap();
        {
            db.apply_batch(&mut db_batch).unwrap();
        }
        db_batch
            .insert_raw("rawkey".as_bytes(), "rawvalue".as_bytes())
            .unwrap();
        {
            let tree = db.open_tree(DbTrees::Binary("rawkeys".as_bytes())).unwrap();
            tree.apply_batch(&mut db_batch).unwrap();
            tree.flush_async().await.unwrap();
            assert_eq!(tree.len(), 1);
        }
        db.flush().unwrap();
        db.flush_async().await.unwrap();
    }
    .await;
    async {
        let foobar_values = db.list_values(DbTrees::Custom("foobar")).unwrap();
        assert_eq!(foobar_values.len(), 2);
        let test_data_one: TestData = db
            .open_tree(DbTrees::Custom("foobar"))
            .unwrap()
            .deserialize(foobar_values[0].0.clone())
            .unwrap();
        assert_eq!(test_data_one.key, "key1".to_string());
        assert_eq!(test_data_one.foo, "foo1".to_string());
        let test_data_two: TestData = db
            .open_tree(DbTrees::Custom("foobar"))
            .unwrap()
            .deserialize(foobar_values[1].0.clone())
            .unwrap();
        assert_eq!(test_data_two.key, "key2".to_string());
        assert_eq!(test_data_two.foo, "foo2".to_string());
        let foobarbaz_values = db.list_values(DbTrees::Custom("foobarbaz")).unwrap();
        assert_eq!(foobarbaz_values.len(), 1);
        let test_data_three: TestData = db
            .open_tree(DbTrees::Custom("foobarbaz"))
            .unwrap()
            .deserialize(foobarbaz_values[0].0.clone())
            .unwrap();
        assert_eq!(test_data_three.key, "key3".to_string());
        assert_eq!(test_data_three.foo, "foo3".to_string());
        let default_tree_values = db.list_values(DbTrees::Default).unwrap();
        assert_eq!(default_tree_values.len(), 1);
        let raw_values = db
            .list_values(DbTrees::Binary("rawkeys".as_bytes()))
            .unwrap();
        assert_eq!(raw_values.len(), 1);
        assert_eq!(raw_values[0].0, "rawkey".as_bytes());
        assert_eq!(raw_values[0].1, "rawvalue".as_bytes());
    }
    .await;
    db.destroy();
    std::fs::remove_dir_all("test_db_basic_async.db").unwrap();
}
