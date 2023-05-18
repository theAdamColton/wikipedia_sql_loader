use bzip2::read::MultiBzDecoder;
use chrono::{DateTime,Utc};
use std::{io::BufReader, fs::File};
use wikipedia_undumper::Undumper;
use mysql::*;
use mysql::prelude::*;
use clap::Parser;


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args{
    /// db username
    #[arg(long)]
    username: String,
    /// db password
    #[arg(long)]
    password: String,
    #[arg(long)]
    db_name: String,
    #[arg(long, default_value_t={"localhost".to_string()})]
    host: String,
    #[arg(long)]
    dumpfilepath: String,
    #[arg(long, default_value_t=2048)]
    batchsize: usize,
}

fn main() {
    let args = Args::parse();

    let opts = OptsBuilder::new()
        .user(Some(args.username))
        .pass(Some(args.password))
        .db_name(Some(args.db_name))
        .ip_or_hostname(Some(args.host));

    let pool = Pool::new(opts).unwrap();
    let mut conn = pool.get_conn().unwrap();

    // This adds the `main` role and default content model
    conn.query_drop(r"
    INSERT IGNORE INTO slot_roles (role_id, role_name)
                        VALUES    (1, 'main');
        ").unwrap();
    conn.query_drop(r"
    INSERT IGNORE INTO content_models (model_id, model_name)
                        VALUES    (1, 'wikitext');
        ").unwrap();

    
    let f = File::open(args.dumpfilepath).unwrap();
    let decompressor = MultiBzDecoder::new(f);
    let bufreader = BufReader::new(decompressor);
    let wikipedia_undumper = Undumper::from_reader(bufreader);

    let mut insert_buffer: Vec<wikipedia_undumper::schema::Page> = Vec::new();

    // hacky variable that we assume the inserted auto increment text ids will follow
    let mut current_max_text_id:usize = conn.query("SELECT MAX(old_id) from text;").unwrap()[0];

    for page_result in wikipedia_undumper.into_iter() {
        let page = page_result.unwrap();
        insert_buffer.push(page);
        

        if (insert_buffer.len() % args.batchsize) == 0 {
            let mut tx = conn.start_transaction(TxOpts::default()).unwrap();

            tx.exec_batch(r"
            INSERT INTO page (
                page_title,  page_namespace,  page_id,  page_is_redirect, page_random, page_touched, page_latest, page_len)
            VALUES
              (:page_title, :page_namespace, :page_id, :page_is_redirect, 0.42,        0,           :page_latest,:page_len)
            ",
            insert_buffer.iter().map(|p| params! {
                "page_title" => &p.title,
                "page_namespace" => p.ns.to_int(),
                "page_id" => p.id,
                "page_is_redirect" => p.redirect.is_some(),
                "page_latest" => p.revisions[0].id,
                "page_len" => p.revisions[0].text.bytes,
                })
            ).unwrap();

            tx.exec_batch(r"
            INSERT IGNORE INTO actor (actor_id, actor_name)
            VALUES           (:actor_id, :actor_name);
            ",
            insert_buffer.iter().map(|p| {
                let rev = &p.revisions[0];
                params! {
                    "actor_id" => rev.contributor.id,
                    "actor_name" => if rev.contributor.ip.is_some() {&rev.contributor.ip} else {&rev.contributor.username},
            }})).unwrap();

            // TODO rev_actor is set to some dummy value
            tx.exec_batch(r"
            INSERT INTO revision (
                rev_id,  rev_page,rev_comment_id,   rev_actor,     rev_timestamp, rev_minor_edit, rev_parent_id, rev_sha1
            ) VALUES ( 
               :rev_id, :rev_page,            1,            1,    :rev_timestamp,:rev_minor_edit,:rev_parent_id,:rev_sha1
            )
            ",
            insert_buffer.iter().map(|p| {
                let rev = &p.revisions[0];
                let date = DateTime::parse_from_rfc3339(&rev.timestamp).unwrap();
                //let date = DateTime::parse_from_str(&rev.timestamp, "%Y-%m-%dT%H:%M:%SZ").unwrap();
                params! {
                    "rev_id" => rev.id,
                    "actor_id" => rev.contributor.id,
                    "rev_page" => p.id,
                    "rev_timestamp" => date.naive_utc().timestamp(),
                    "rev_minor_edit" => rev.minor.is_some(),
                    "rev_parent_id" => rev.parentid,
                    "rev_sha1" => &rev.sha1,
                }
            })).unwrap();

            // revision -> slot -> content -> old text
            tx.exec_batch(r"
            INSERT INTO text (old_text, old_flags)
            VALUES (
                             :text,     'utf-8'
            );
            ", insert_buffer.iter().map(|p| {
                let rev = &p.revisions[0];
                params! {
                    "text" => &rev.text.text,
                }})
            ).unwrap();

            tx.exec_batch(r"
            INSERT INTO content(content_size, content_sha1, content_model, content_address)
            VALUES (           :content_size,:content_sha1,             1, CONCAT('tt:', :text_id)
            );
            ", insert_buffer.iter().enumerate().map(|(i, p)| {
                let rev = &p.revisions[0];
                params! {
                    "content_size" => rev.text.bytes,
                    "content_sha1" => "",
                    "text_id" => current_max_text_id+i,
                }})).unwrap();

            tx.exec_batch(r"
            INSERT INTO slots(slot_revision_id, slot_role_id, slot_content_id, slot_origin)
            VALUES (         :slot_revision_id,            1,               1,:slot_origin)
            ", insert_buffer.iter().map(|p| {
                let rev = &p.revisions[0];
                params! {
                    "slot_revision_id" => rev.id,
                    "slot_origin" => rev.id,
                }
            })).unwrap();


            tx.commit().unwrap();

            current_max_text_id += insert_buffer.len();
            insert_buffer.clear();
        }
    }
}
