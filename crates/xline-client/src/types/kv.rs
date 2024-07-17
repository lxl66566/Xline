use crate::error::Result;
use curp::client::ClientApi;
use futures::{future::BoxFuture, FutureExt};
use pin_project::pin_project;
use std::future::Future;
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

/// The type of the future inside [`PutFut`].
type RequestFutureType<'a> = Option<
    BoxFuture<
        'a,
        std::result::Result<
            std::result::Result<(CommandResponse, Option<SyncResponse>), ExecuteError>,
            Status,
        >,
    >,
>;

/// Future for `Put`, make it awaitable.
///
/// Before first poll, fut will be [`Option::None`].
/// Once It's been polled, fut will be [`Option::Some`] and inner will be [`Option::None`].
#[pin_project]
pub struct PutFut<'a> {
    #[pin]
    /// The future to be polled.
    fut: RequestFutureType<'a>,
    /// The inner request, to be constructed to a [`Command`].
    inner: Option<xlineapi::PutRequest>,
    /// The curp_client to be used for sending the request.
    curp_client: &'a Arc<dyn ClientApi<Error = Status, Cmd = Command> + Send + Sync>,
    /// The token to be used for authentication.
    token: Option<&'a String>,
}

impl<'a> PutFut<'a> {
    #[inline]
    #[must_use]
    /// `key` is the key, in bytes, to put into the key-value store.
    /// `value` is the value, in bytes, to associate with the key in the key-value store.
    pub fn new(
        curp_client: &'a Arc<dyn ClientApi<Error = Status, Cmd = Command> + Send + Sync>,
        token: Option<&'a String>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Self {
        Self {
            curp_client,
            token,
            fut: None,
            inner: Some(xlineapi::PutRequest {
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
        self.inner = self.inner.map(|mut inner| {
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
        self.inner = self.inner.map(|mut inner| {
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
        self.inner = self.inner.map(|mut inner| {
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
        self.inner = self.inner.map(|mut inner| {
            inner.ignore_lease = ignore_lease;
            inner
        });
        self
    }
}

impl std::fmt::Debug for PutFut<'_> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutFut").finish()
    }
}

impl Future for PutFut<'_> {
    type Output = Result<PutResponse>;

    /// Poll the inner future constructed by [`xlineapi::PutRequest`].
    ///
    /// # panic
    ///
    /// panic if [`Self::inner`] is [`None`].
    #[inline]
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.fut.is_none() {
            let cmd = Command::new(RequestWrapper::from(
                self.inner
                    .take()
                    .unwrap_or_else(|| panic!("inner request should be constructed")),
            ));
            let client = Arc::clone(self.curp_client);
            let token = self.token;
            let fut = async move { client.propose(&cmd, token, true).await };
            self.fut = Some(Box::pin(fut));
        }
        match self
            .fut
            .as_mut()
            .unwrap_or_else(|| unreachable!("`self.fut` must not be none"))
            .poll_unpin(cx)
        {
            // std::task::Poll::Ready(Ok(cmd_res)) => std::task::Poll::Ready(Ok(cmd_res??.into())),
            std::task::Poll::Ready(res) => std::task::Poll::Ready(Ok(res??.0.into_inner().into())),
            // std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e.into())),
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
pub struct Compare(xlineapi::Compare);

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

/// Transaction operation.
#[derive(Debug, Clone, PartialEq)]
pub struct TxnOp {
    /// The inner txn op request
    inner: xlineapi::Request,
}

impl TxnOp {
    /// Creates a `Put` operation.
    #[inline]
    #[must_use]
    pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        TxnOp {
            inner: xlineapi::Request::RequestPut(
                option
                    .unwrap_or_default()
                    .with_kv(key.into(), value.into())
                    .into(),
            ),
        }
    }

    /// Creates a `Range` operation.
    #[inline]
    #[must_use]
    pub fn range(request: RangeRequest) -> Self {
        TxnOp {
            inner: xlineapi::Request::RequestRange(request.into()),
        }
    }

    /// Creates a `DeleteRange` operation.
    #[inline]
    #[must_use]
    pub fn delete(request: DeleteRangeRequest) -> Self {
        TxnOp {
            inner: xlineapi::Request::RequestDeleteRange(request.into()),
        }
    }

    /// Creates a `Txn` operation.
    #[inline]
    #[must_use]
    pub fn txn(txn: TxnRequest) -> Self {
        TxnOp {
            inner: xlineapi::Request::RequestTxn(txn.into()),
        }
    }
}

impl From<TxnOp> for xlineapi::Request {
    #[inline]
    fn from(op: TxnOp) -> Self {
        op.inner
    }
}

/// Transaction of multiple operations.
#[derive(Debug)]
pub struct TxnRequest {
    /// the inner txn request
    pub(crate) inner: xlineapi::TxnRequest,
    /// If `when` have be set
    c_when: bool,
    /// If `then` have be set
    c_then: bool,
    /// If `else` have be set
    c_else: bool,
}

impl TxnRequest {
    /// Creates a new transaction.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: xlineapi::TxnRequest {
                compare: Vec::new(),
                success: Vec::new(),
                failure: Vec::new(),
            },
            c_when: false,
            c_then: false,
            c_else: false,
        }
    }

    /// Takes a list of comparison. If all comparisons passed in succeed,
    /// the operations passed into `and_then()` will be executed. Or the operations
    /// passed into `or_else()` will be executed.
    ///
    /// # Panics
    ///
    /// panics if `when` is called twice or called after `when` or called after `or_else`
    #[inline]
    #[must_use]
    pub fn when(mut self, compares: impl Into<Vec<Compare>>) -> Self {
        assert!(!self.c_when, "cannot call when twice");
        assert!(!self.c_then, "cannot call when after and_then");
        assert!(!self.c_else, "cannot call when after or_else");

        let compares_vec: Vec<Compare> = compares.into();
        self.c_when = true;
        self.inner.compare = compares_vec.into_iter().map(|c| c.0).collect();
        self
    }

    /// Takes a list of operations. The operations list will be executed, if the
    /// comparisons passed in `when()` succeed.
    ///
    /// # Panics
    ///
    /// panics if `and_then` is called twice or called after `or_else`
    #[inline]
    #[must_use]
    pub fn and_then(mut self, operations: impl Into<Vec<TxnOp>>) -> Self {
        assert!(!self.c_then, "cannot call and_then twice");
        assert!(!self.c_else, "cannot call and_then after or_else");

        self.c_then = true;
        self.inner.success = operations
            .into()
            .into_iter()
            .map(|op| xlineapi::RequestOp {
                request: Some(op.into()),
            })
            .collect();
        self
    }

    /// Takes a list of operations. The operations list will be executed, if the
    /// comparisons passed in `when()` fail.
    ///
    /// # Panics
    ///
    /// panics if `or_else` is called twice
    #[inline]
    #[must_use]
    pub fn or_else(mut self, operations: impl Into<Vec<TxnOp>>) -> Self {
        assert!(!self.c_else, "cannot call or_else twice");

        self.c_else = true;
        self.inner.failure = operations
            .into()
            .into_iter()
            .map(|op| xlineapi::RequestOp {
                request: Some(op.into()),
            })
            .collect();
        self
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
