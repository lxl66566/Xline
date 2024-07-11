use anyhow::Result;
use clap::{arg, value_parser, ArgMatches, Command};
use xline_client::{types::kv::PutFut, Client};

use crate::utils::printer::Printer;

/// Indicates the type of the request builted.
/// The first `Vec<u8>` is the key, the second `Vec<u8>` is the value,
/// and the third is the options.
type PutRequest = (Vec<u8>, Vec<u8>, PutFut);

/// Definition of `get` command
pub(crate) fn command() -> Command {
    Command::new("put")
        .about("Puts the given key into the store")
        .arg(arg!(<key> "The key"))
        // TODO: support reading value from stdin
        .arg(arg!(<value> "The value"))
        .arg(
            arg!(--lease <ID> "lease ID to attach to the key")
                .value_parser(value_parser!(i64))
                .default_value("0"),
        )
        .arg(arg!(--prev_kv "return the previous key-value pair before modification"))
        .arg(arg!(--ignore_value "updates the key using its current value"))
        .arg(arg!(--ignore_lease "updates the key using its current lease"))
}

/// Execute the command
pub(crate) async fn execute(client: &mut Client, matches: &ArgMatches) -> Result<()> {
    let key = matches.get_one::<String>("key").expect("required");
    let value = matches.get_one::<String>("value").expect("required");
    let lease = matches.get_one::<i64>("lease").expect("required");
    let prev_kv = matches.get_flag("prev_kv");
    let ignore_value = matches.get_flag("ignore_value");
    let ignore_lease = matches.get_flag("ignore_lease");

    let resp = client
        .kv_client()
        .put(key.as_bytes().to_vec(), value.as_bytes().to_vec())
        .with_lease(*lease)
        .with_prev_kv(prev_kv)
        .with_ignore_value(ignore_value)
        .with_ignore_lease(ignore_lease)
        .await?;

    resp.print();

    Ok(())
}
