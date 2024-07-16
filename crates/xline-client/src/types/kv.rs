use crate::error::Result;
use curp::client::ClientApi;
use futures::FutureExt;
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::{pin::pin, sync::Arc};
use tonic::Status;
use xlineapi::{
    command::{Command, CommandResponse, KeyRange, SyncResponse},
    execute_error::ExecuteError,
    RequestWrapper,
};
pub use xlineapi::{
    CompactionResponse, CompareResult, CompareTarget, DeleteRangeResponse, PutResponse,
    RangeResponse, Response, ResponseOp, SortOrder, SortTarget, TargetUnion, TxnResponse,
};

/// The type of Response
type ResponseType = std::result::Result<
    std::result::Result<(CommandResponse, Option<SyncResponse>), ExecuteError>,
    Status,
>;

/// The type of the future inside [`PutFut`].
// type RequestFutureType<'a> = Option<BoxFuture<'a, ResponseType>>;
type RequestFutureType = Option<Pin<Box<dyn Future<Output = ResponseType> + Send>>>;

/// Future for `Put`, make it awaitable.
///
/// Before first poll, fut will be [`Option::None`].
/// Once It's been polled, fut will be [`Option::Some`] and inner will be [`Option::None`].
#[pin_project]
pub struct PutFut {
    #[pin]
    /// The future to be polled.
    fut: RequestFutureType,
    /// The inner request, to be constructed to a [`Command`].
    request: Option<xlineapi::PutRequest>,
    /// The curp_client to be used for sending the request.
    curp_client: Arc<dyn ClientApi<Error = Status, Cmd = Command> + Send + Sync>,
    /// The token to be used for authentication.
    token: Option<String>,
}

impl PutFut {
    #[inline]
    #[must_use]
    /// `key` is the key, in bytes, to put into the key-value store.
    /// `value` is the value, in bytes, to associate with the key in the key-value store.
    pub fn new(
        curp_client: Arc<dyn ClientApi<Error = Status, Cmd = Command> + Send + Sync>,
        token: Option<String>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Self {
        Self {
            curp_client,
            token,
            fut: None,
            request: Some(xlineapi::PutRequest {
                key,
                value,
                ..Default::default()
            }),
        }
    }

    /// lease is the lease ID to associate with the key in the key-value store.
    /// A lease value of 0 indicates no lease.
    #[inline]
    #[must_use]
    pub fn with_lease(mut self, lease: i64) -> Self {
        self.request = self.request.map(|mut inner| {
            inner.lease = lease;
            inner
        });
        self
    }

    /// If `prev_kv` is set, Xline gets the previous key-value pair before changing it.
    /// The previous key-value pair will be returned in the put response.
    #[inline]
    #[must_use]
    pub fn with_prev_kv(mut self, prev_kv: bool) -> Self {
        self.request = self.request.map(|mut inner| {
            inner.prev_kv = prev_kv;
            inner
        });
        self
    }

    /// If `ignore_value` is set, Xline updates the key using its current value.
    /// Returns an error if the key does not exist.
    #[inline]
    #[must_use]
    pub fn with_ignore_value(mut self, ignore_value: bool) -> Self {
        self.request = self.request.map(|mut inner| {
            inner.ignore_value = ignore_value;
            inner
        });
        self
    }

    /// If `ignore_lease` is set, Xline updates the key using its current lease.
    /// Returns an error if the key does not exist.
    #[inline]
    #[must_use]
    pub fn with_ignore_lease(mut self, ignore_lease: bool) -> Self {
        self.request = self.request.map(|mut inner| {
            inner.ignore_lease = ignore_lease;
            inner
        });
        self
    }

    /// Take the inner [`xlineapi::PutRequest`] out.
    #[inline]
    pub fn take_request(&mut self) -> Option<xlineapi::PutRequest> {
        self.request.take()
    }
}

impl std::fmt::Debug for PutFut {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutFut")
            .field("inner", &self.request)
            .finish()
    }
}

impl Future for PutFut {
    type Output = Result<PutResponse>;

    /// Poll the inner future constructed by [`xlineapi::PutRequest`].
    ///
    /// # panic
    ///
    /// panic if [`Self::inner`] is [`None`].
    #[inline]
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.fut.is_none() {
            let cmd = Command::new(RequestWrapper::from(
                self.take_request()
                    .unwrap_or_else(|| panic!("inner request should be constructed")),
            ));
            let client = Arc::clone(&self.curp_client);
            let token = self.token.clone();
            self.fut = Some(Box::pin(async move {
                client.propose(&cmd, token.as_ref(), true).await
            }));
        }
        match self
            .fut
            .as_mut()
            .unwrap_or_else(|| unreachable!("`self.fut` must not be none"))
            .poll_unpin(cx)
        {
            std::task::Poll::Ready(res) => std::task::Poll::Ready(Ok(res??.0.into_inner().into())),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// Request type for `Range`
#[derive(Debug, PartialEq)]
pub struct RangeRequest {
    /// Inner request
    inner: xlineapi::RangeRequest,
}

impl RangeRequest {
    /// Creates a new `RangeRequest`
    ///
    /// `key` is the first key for the range. If `range_end` is not given, the request only looks up key.
    #[inline]
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: xlineapi::RangeRequest {
                key: key.into(),
                ..Default::default()
            },
        }
    }

    /// If set, Xline will return all keys with the matching prefix
    #[inline]
    #[must_use]
    pub fn with_prefix(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
            self.inner.range_end = vec![0];
        } else {
            self.inner.range_end = KeyRange::get_prefix(&self.inner.key);
        }
        self
    }

    /// If set, Xline will return all keys that are equal or greater than the given key
    #[inline]
    #[must_use]
    pub fn with_from_key(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
        }
        self.inner.range_end = vec![0];
        self
    }

    /// `range_end` is the upper bound on the requested range \[key,` range_en`d).
    /// If `range_end` is '\0', the range is all keys >= key.
    #[inline]
    #[must_use]
    pub fn with_range_end(mut self, range_end: impl Into<Vec<u8>>) -> Self {
        self.inner.range_end = range_end.into();
        self
    }

    /// `limit` is a limit on the number of keys returned for the request. When limit is set to 0,
    /// it is treated as no limit.
    #[inline]
    #[must_use]
    pub fn with_limit(mut self, limit: i64) -> Self {
        self.inner.limit = limit;
        self
    }

    /// `revision` is the point-in-time of the key-value store to use for the range.
    /// If revision is less or equal to zero, the range is over the newest key-value store.
    #[inline]
    #[must_use]
    pub fn with_revision(mut self, revision: i64) -> Self {
        self.inner.revision = revision;
        self
    }

    /// Sets the sort order for returned keys
    #[inline]
    #[must_use]
    #[allow(clippy::as_conversions)] // this case is always safe
    pub fn with_sort_order(mut self, sort_order: SortOrder) -> Self {
        self.inner.sort_order = sort_order as i32;
        self
    }

    /// Sets the sort target for returned keys
    #[inline]
    #[must_use]
    #[allow(clippy::as_conversions)] // this case is always safe
    pub fn with_sort_target(mut self, sort_target: SortTarget) -> Self {
        self.inner.sort_target = sort_target as i32;
        self
    }

    /// serializable sets the range request to use serializable member-local reads.
    /// Range requests are linearizable by default; linearizable requests have higher
    /// latency and lower throughput than serializable requests but reflect the current
    /// consensus of the cluster. For better performance, in exchange for possible stale reads,
    /// a serializable range request is served locally without needing to reach consensus
    /// with other nodes in the cluster.
    #[inline]
    #[must_use]
    pub fn with_serializable(mut self, serializable: bool) -> Self {
        self.inner.serializable = serializable;
        self
    }

    /// If set, Xline will return only the keys
    #[inline]
    #[must_use]
    pub fn with_keys_only(mut self, keys_only: bool) -> Self {
        self.inner.keys_only = keys_only;
        self
    }

    /// If set, Xline will return only the count of the keys
    #[inline]
    #[must_use]
    pub fn with_count_only(mut self, count_only: bool) -> Self {
        self.inner.count_only = count_only;
        self
    }

    /// `min_mod_revision` is the lower bound for returned key mod revisions; all keys with
    /// lesser mod revisions will be filtered away.
    #[inline]
    #[must_use]
    pub fn with_min_mod_revision(mut self, min_mod_revision: i64) -> Self {
        self.inner.min_mod_revision = min_mod_revision;
        self
    }

    /// `max_mod_revision` is the upper bound for returned key mod revisions; all keys with
    /// greater mod revisions will be filtered away.
    #[inline]
    #[must_use]
    pub fn with_max_mod_revision(mut self, max_mod_revision: i64) -> Self {
        self.inner.max_mod_revision = max_mod_revision;
        self
    }

    /// `min_create_revision` is the lower bound for returned key create revisions; all keys with
    /// lesser create revisions will be filtered away.
    #[inline]
    #[must_use]
    pub fn with_min_create_revision(mut self, min_create_revision: i64) -> Self {
        self.inner.min_create_revision = min_create_revision;
        self
    }

    /// `max_create_revision` is the upper bound for returned key create revisions; all keys with
    /// greater create revisions will be filtered away.
    #[inline]
    #[must_use]
    pub fn with_max_create_revision(mut self, max_create_revision: i64) -> Self {
        self.inner.max_create_revision = max_create_revision;
        self
    }

    /// Get `key`
    #[inline]
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.inner.key
    }

    /// Get `range_end`
    #[inline]
    #[must_use]
    pub fn range_end(&self) -> &[u8] {
        &self.inner.range_end
    }

    /// Get `limit`
    #[inline]
    #[must_use]
    pub fn limit(&self) -> i64 {
        self.inner.limit
    }

    /// Get `revision`
    #[inline]
    #[must_use]
    pub fn revision(&self) -> i64 {
        self.inner.revision
    }

    /// Get `sort_order`
    #[inline]
    #[must_use]
    pub fn sort_order(&self) -> i32 {
        self.inner.sort_order
    }

    /// Get `sort_target`
    #[inline]
    #[must_use]
    pub fn sort_target(&self) -> i32 {
        self.inner.sort_target
    }

    /// Get `serializable`
    #[inline]
    #[must_use]
    pub fn serializable(&self) -> bool {
        self.inner.serializable
    }

    /// Get `keys_only`
    #[inline]
    #[must_use]
    pub fn keys_only(&self) -> bool {
        self.inner.keys_only
    }

    /// Get `count_only`
    #[inline]
    #[must_use]
    pub fn count_only(&self) -> bool {
        self.inner.count_only
    }

    /// Get `min_mod_revision`
    #[inline]
    #[must_use]
    pub fn min_mod_revision(&self) -> i64 {
        self.inner.min_mod_revision
    }

    /// Get `max_mod_revision`
    #[inline]
    #[must_use]
    pub fn max_mod_revision(&self) -> i64 {
        self.inner.max_mod_revision
    }

    /// Get `min_create_revision`
    #[inline]
    #[must_use]
    pub fn min_create_revision(&self) -> i64 {
        self.inner.min_create_revision
    }

    /// Get `max_create_revision`
    #[inline]
    #[must_use]
    pub fn max_create_revision(&self) -> i64 {
        self.inner.max_create_revision
    }
}

impl From<RangeRequest> for xlineapi::RangeRequest {
    #[inline]
    fn from(req: RangeRequest) -> Self {
        req.inner
    }
}

/// Request type for `DeleteRange`
#[derive(Debug, PartialEq)]
pub struct DeleteRangeRequest {
    /// Inner request
    inner: xlineapi::DeleteRangeRequest,
}

impl DeleteRangeRequest {
    /// Creates a new `DeleteRangeRequest`
    ///
    /// `key` is the first key to delete in the range.
    #[inline]
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: xlineapi::DeleteRangeRequest {
                key: key.into(),
                ..Default::default()
            },
        }
    }

    /// If set, Xline will delete all keys with the matching prefix
    #[inline]
    #[must_use]
    pub fn with_prefix(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
            self.inner.range_end = vec![0];
        } else {
            self.inner.range_end = KeyRange::get_prefix(&self.inner.key);
        }
        self
    }

    /// If set, Xline will delete all keys that are equal to or greater than the given key
    #[inline]
    #[must_use]
    pub fn with_from_key(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
        }
        self.inner.range_end = vec![0];
        self
    }

    /// `range_end` is the key following the last key to delete for the range \[key,` range_en`d).
    /// If `range_end` is not given, the range is defined to contain only the key argument.
    /// If `range_end` is one bit larger than the given key, then the range is all the keys
    /// with the prefix (the given key).
    /// If `range_end` is '\0', the range is all keys greater than or equal to the key argument.
    #[inline]
    #[must_use]
    pub fn with_range_end(mut self, range_end: impl Into<Vec<u8>>) -> Self {
        self.inner.range_end = range_end.into();
        self
    }

    /// If `prev_kv` is set, Xline gets the previous key-value pairs before deleting it.
    /// The previous key-value pairs will be returned in the delete response.
    #[inline]
    #[must_use]
    pub fn with_prev_kv(mut self, prev_kv: bool) -> Self {
        self.inner.prev_kv = prev_kv;
        self
    }

    /// Get `key`
    #[inline]
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.inner.key
    }

    /// Get `range_end`
    #[inline]
    #[must_use]
    pub fn range_end(&self) -> &[u8] {
        &self.inner.range_end
    }

    /// Get `prev_kv`
    #[inline]
    #[must_use]
    pub fn prev_kv(&self) -> bool {
        self.inner.prev_kv
    }
}

impl From<DeleteRangeRequest> for xlineapi::DeleteRangeRequest {
    #[inline]
    fn from(req: DeleteRangeRequest) -> Self {
        req.inner
    }
}

/// Transaction comparison.
#[derive(Clone, Debug, PartialEq)]
pub struct Compare(pub(crate) xlineapi::Compare);

impl Compare {
    /// Creates a new `Compare`.
    ///
    /// `key` is the subject key for the comparison operation.
    /// `cmp` is logical comparison operation for this comparison.
    /// `target` is the key-value field to inspect for the comparison.
    /// `target_union` is the union that wrap the target value
    #[inline]
    fn new(
        key: impl Into<Vec<u8>>,
        cmp: CompareResult,
        target: CompareTarget,
        target_union: TargetUnion,
    ) -> Self {
        Self(xlineapi::Compare {
            result: cmp.into(),
            target: target.into(),
            key: key.into(),
            range_end: Vec::new(),
            target_union: Some(target_union),
        })
    }

    /// Compares the version of the given key.
    #[inline]
    pub fn version(key: impl Into<Vec<u8>>, cmp: CompareResult, version: i64) -> Self {
        Self::new(
            key,
            cmp,
            CompareTarget::Version,
            TargetUnion::Version(version),
        )
    }

    /// Compares the creation revision of the given key.
    #[inline]
    pub fn create_revision(key: impl Into<Vec<u8>>, cmp: CompareResult, revision: i64) -> Self {
        Self::new(
            key,
            cmp,
            CompareTarget::Create,
            TargetUnion::CreateRevision(revision),
        )
    }

    /// Compares the last modified revision of the given key.
    #[inline]
    pub fn mod_revision(key: impl Into<Vec<u8>>, cmp: CompareResult, revision: i64) -> Self {
        Self::new(
            key,
            cmp,
            CompareTarget::Mod,
            TargetUnion::ModRevision(revision),
        )
    }

    /// Compares the value of the given key.
    #[inline]
    pub fn value(key: impl Into<Vec<u8>>, cmp: CompareResult, value: impl Into<Vec<u8>>) -> Self {
        Self::new(
            key,
            cmp,
            CompareTarget::Value,
            TargetUnion::Value(value.into()),
        )
    }

    /// Compares the lease id of the given key.
    #[inline]
    pub fn lease(key: impl Into<Vec<u8>>, cmp: CompareResult, lease: i64) -> Self {
        Self::new(key, cmp, CompareTarget::Lease, TargetUnion::Lease(lease))
    }

    /// Sets the comparison to scan the range [key, end).
    #[inline]
    #[must_use]
    pub fn with_range(mut self, end: impl Into<Vec<u8>>) -> Self {
        self.0.range_end = end.into();
        self
    }

    /// Sets the comparison to scan all keys prefixed by the key.
    #[inline]
    #[must_use]
    pub fn with_prefix(mut self) -> Self {
        self.0.range_end = KeyRange::get_prefix(&self.0.key);
        self
    }
}

/// Transaction operation
#[derive(Debug)]
#[non_exhaustive]
pub enum TxnOp {
    /// Request, used by old version. the Request will be replaced later.
    Request(xlineapi::Request),
    /// Future, used by new version
    Future(PutFut),
}

impl From<xlineapi::Request> for TxnOp {
    #[inline]
    fn from(value: xlineapi::Request) -> Self {
        Self::Request(value)
    }
}

impl From<PutFut> for TxnOp {
    #[inline]
    fn from(value: PutFut) -> Self {
        Self::Future(value)
    }
}

impl From<TxnOp> for xlineapi::Request {
    #[inline]
    fn from(value: TxnOp) -> Self {
        match value {
            TxnOp::Request(request) => request,
            TxnOp::Future(mut fut) => xlineapi::Request::RequestPut(
                fut.take_request()
                    .unwrap_or_else(|| panic!("txn op future is not ready")),
            ),
        }
    }
}

// /// Transaction operation.
// #[derive(Debug, Clone, PartialEq)]
// pub struct TxnOp<'a>(TxnOp<'a>);

impl TxnOp {
    /// Creates a `Range` operation.
    #[inline]
    #[must_use]
    pub fn range(request: RangeRequest) -> Self {
        xlineapi::Request::RequestRange(request.into()).into()
    }

    /// Creates a `DeleteRange` operation.
    #[inline]
    #[must_use]
    pub fn delete(request: DeleteRangeRequest) -> Self {
        xlineapi::Request::RequestDeleteRange(request.into()).into()
    }

    /// Creates a `Txn` operation.
    #[inline]
    #[must_use]
    pub fn txn(txn: TxnRequest) -> Self {
        xlineapi::Request::RequestTxn(txn.into()).into()
    }
}

/// Transaction of multiple operations.
#[derive(Debug, Default, Clone)]
pub struct TxnRequest {
    /// the inner txn request
    pub(crate) inner: xlineapi::TxnRequest,
}

impl TxnRequest {
    /// Takes a comparison and append it to inner compare Vec. If all comparisons passed in succeed,
    /// the operations passed into `and_then()` will be executed. Or the operations
    /// passed into `or_else()` will be executed.
    #[inline]
    pub fn when(&mut self, compare: impl Into<Compare>) {
        self.inner.compare.push(compare.into().0);
    }

    /// Append an operation to inner success Vec. The operations list will be executed,
    /// if the comparisons passed in `when()` succeed.
    #[inline]
    pub fn and_then(&mut self, operation: impl Into<TxnOp>) {
        let temp = operation.into();
        self.inner.success.push(xlineapi::RequestOp {
            request: Some(temp.into()),
        });
    }

    /// Append an operation to inner failure Vec. The operations list will be executed, if the
    /// comparisons passed in `when()` fail.
    #[inline]
    pub fn or_else(&mut self, operation: impl Into<TxnOp>) {
        self.inner.failure.push(xlineapi::RequestOp {
            request: Some(operation.into().into()),
        });
    }
}

impl From<TxnRequest> for xlineapi::TxnRequest {
    #[inline]
    fn from(txn: TxnRequest) -> Self {
        txn.inner
    }
}

/// Compaction Request compacts the key-value store up to a given revision.
/// All keys with revisions less than the given revision will be compacted.
/// The compaction process will remove all historical versions of these keys, except for the most recent one.
/// For example, here is a revision list: [(A, 1), (A, 2), (A, 3), (A, 4), (A, 5)].
/// We compact at revision 3. After the compaction, the revision list will become [(A, 3), (A, 4), (A, 5)].
/// All revisions less than 3 are deleted. The latest revision, 3, will be kept.
#[derive(Debug, PartialEq)]
pub struct CompactionRequest {
    /// The inner request
    inner: xlineapi::CompactionRequest,
}

impl CompactionRequest {
    /// Creates a new `CompactionRequest`
    ///
    /// `Revision` is the key-value store revision for the compaction operation.
    #[inline]
    #[must_use]
    pub fn new(revision: i64) -> Self {
        Self {
            inner: xlineapi::CompactionRequest {
                revision,
                ..Default::default()
            },
        }
    }

    /// Physical is set so the RPC will wait until the compaction is physically
    /// applied to the local database such that compacted entries are totally
    /// removed from the backend database.
    #[inline]
    #[must_use]
    pub fn with_physical(mut self) -> Self {
        self.inner.physical = true;
        self
    }

    /// Get `physical`
    #[inline]
    #[must_use]
    pub fn physical(&self) -> bool {
        self.inner.physical
    }
}

impl From<CompactionRequest> for xlineapi::CompactionRequest {
    #[inline]
    fn from(req: CompactionRequest) -> Self {
        req.inner
    }
}
