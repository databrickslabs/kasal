"""A router may take more than one branch.

Returning a single route name could only ever run one branch, and on a batch
that genuinely contains both politics and sports the loser was decided by dict
ordering — silently, and never revisited. The runtime turns a router's return
value into a SIGNAL, so a list of names emits several and every route whose
condition held runs.

Prior art: LangGraph conditional edges return a Sequence, Airflow's branch
operator returns a list of task_ids, n8n's Switch has "send to all matching
outputs".
"""

import pytest

from src.services.flow_builder.runtime import Flow, and_, listen, or_, router, start


class TestSeveralRoutesFire:
    @pytest.mark.asyncio
    async def test_every_matching_route_runs(self):
        ran: list[str] = []

        class MultiRouteFlow(Flow):
            @start()
            def classify(self):
                return "batch"

            @router(classify)
            def choose(self, _previous=None):
                return ["route_politics", "route_sports"]

            @listen("route_politics")
            def politics(self, _previous=None):
                ran.append("politics")

            @listen("route_sports")
            def sports(self, _previous=None):
                ran.append("sports")

        await MultiRouteFlow().kickoff_async()

        assert sorted(ran) == ["politics", "sports"]

    @pytest.mark.asyncio
    async def test_only_the_matching_route_runs(self):
        ran: list[str] = []

        class OneRouteFlow(Flow):
            @start()
            def classify(self):
                return "batch"

            @router(classify)
            def choose(self, _previous=None):
                return ["route_politics"]

            @listen("route_politics")
            def politics(self, _previous=None):
                ran.append("politics")

            @listen("route_sports")
            def sports(self, _previous=None):
                ran.append("sports")

        await OneRouteFlow().kickoff_async()

        assert ran == ["politics"]

    @pytest.mark.asyncio
    async def test_a_bare_string_still_works(self):
        """The single-route shape predates this and must keep behaving."""
        ran: list[str] = []

        class StringRouteFlow(Flow):
            @start()
            def classify(self):
                return "batch"

            @router(classify)
            def choose(self, _previous=None):
                return "route_politics"

            @listen("route_politics")
            def politics(self, _previous=None):
                ran.append("politics")

        await StringRouteFlow().kickoff_async()

        assert ran == ["politics"]

    @pytest.mark.asyncio
    async def test_no_route_runs_nothing(self):
        ran: list[str] = []

        class NoRouteFlow(Flow):
            @start()
            def classify(self):
                return "batch"

            @router(classify)
            def choose(self, _previous=None):
                return None

            @listen("route_politics")
            def politics(self, _previous=None):
                ran.append("politics")

        await NoRouteFlow().kickoff_async()

        assert ran == []

    @pytest.mark.asyncio
    async def test_non_string_entries_are_ignored(self):
        """A malformed return must not crash the run or emit a bogus signal."""
        ran: list[str] = []

        class MessyRouteFlow(Flow):
            @start()
            def classify(self):
                return "batch"

            @router(classify)
            def choose(self, _previous=None):
                return ["route_politics", None, 42]

            @listen("route_politics")
            def politics(self, _previous=None):
                ran.append("politics")

        await MessyRouteFlow().kickoff_async()

        assert ran == ["politics"]


class TestReturnShape:
    """One match keeps the bare-string shape everything downstream already
    reads; only a genuine multi-match returns a list."""

    @pytest.mark.asyncio
    async def test_both_shapes_reach_their_listeners(self):
        ran: list[str] = []

        class BothShapes(Flow):
            @start()
            def begin(self):
                return "batch"

            @router(begin)
            def single(self, _previous=None):
                return "route_a"

            @listen("route_a")
            def a(self, _previous=None):
                ran.append("a")

            @listen(a)
            def after_a(self, _previous=None):
                ran.append("after_a")

        await BothShapes().kickoff_async()

        assert ran == ["a", "after_a"]


class TestBranchesRunConcurrently:
    """Two matching routes are independent. Awaiting each signal in turn ran a
    whole branch — crew and all — before the next route was even emitted, so
    they executed sequentially and an OR join fired after the first finished
    while the second had not started (observed on execution 8c2d1dd9)."""

    @pytest.mark.asyncio
    async def test_both_branches_are_in_flight_at_once(self):
        import asyncio

        started: list[str] = []
        release = asyncio.Event()

        class ConcurrentFlow(Flow):
            @start()
            def begin(self):
                return "batch"

            @router(begin)
            def choose(self, _previous=None):
                return ["route_a", "route_b"]

            @listen("route_a")
            async def slow_a(self, _previous=None):
                started.append("a")
                await release.wait()

            @listen("route_b")
            async def quick_b(self, _previous=None):
                started.append("b")
                # Proof that b started while a is still blocked.
                release.set()

        await ConcurrentFlow().kickoff_async()

        assert sorted(started) == ["a", "b"]

    @pytest.mark.asyncio
    async def test_an_or_join_sees_both_branches(self):
        """The email-after-the-branches case."""
        order: list[str] = []

        class JoinFlow(Flow):
            @start()
            def begin(self):
                return "batch"

            @router(begin)
            def choose(self, _previous=None):
                return ["route_a", "route_b"]

            @listen("route_a")
            def a(self, _previous=None):
                order.append("a")

            @listen("route_b")
            def b(self, _previous=None):
                order.append("b")

            @listen(or_(a, b))
            def join(self, _previous=None):
                order.append("join")

        await JoinFlow().kickoff_async()

        assert "join" in order
        assert {"a", "b"} <= set(order)


class TestListenersReceiveUpstreamOutput:
    """The generated crew listener is `async def listener_method(self, *results)`.

    *results is VAR_POSITIONAL, which the dispatcher used to exclude when
    deciding whether to pass the upstream output — so the method was called with
    NO arguments, `results` was always empty, and every listener node logged
    "No previous outputs received" and ran with none of its upstream crew's
    work. Route listeners took a NAMED parameter and were unaffected, which is
    why only plain listener nodes were starved (execution 8778d01a: the email
    crew received nothing while both branches had produced decks).
    """

    @pytest.mark.asyncio
    async def test_a_star_args_listener_receives_the_upstream_output(self):
        seen: list[tuple] = []

        class StarArgsFlow(Flow):
            @start()
            def produce(self):
                return "the upstream output"

            @listen(produce)
            async def consume(self, *results):
                seen.append(results)

        await StarArgsFlow().kickoff_async()

        assert seen == [("the upstream output",)]

    @pytest.mark.asyncio
    async def test_a_named_parameter_listener_still_receives_it(self):
        seen: list = []

        class NamedFlow(Flow):
            @start()
            def produce(self):
                return "the upstream output"

            @listen(produce)
            async def consume(self, previous_output):
                seen.append(previous_output)

        await NamedFlow().kickoff_async()

        assert seen == ["the upstream output"]

    @pytest.mark.asyncio
    async def test_a_zero_argument_listener_is_still_called_without_args(self):
        """Taking no parameters must keep working — it must not start raising."""
        ran: list[str] = []

        class NoArgsFlow(Flow):
            @start()
            def produce(self):
                return "ignored"

            @listen(produce)
            async def consume(self):
                ran.append("ran")

        await NoArgsFlow().kickoff_async()

        assert ran == ["ran"]

    @pytest.mark.asyncio
    async def test_an_or_listener_after_a_route_gets_the_branch_output(self):
        """The email-after-the-branches case, end to end."""
        received: list = []

        class BranchJoinFlow(Flow):
            @start()
            def classify(self):
                return "classified"

            @router(classify)
            def choose(self, _previous=None):
                return ["route_a"]

            @listen("route_a")
            async def branch(self, *results):
                return "the deck"

            @listen(branch)
            async def email(self, *results):
                received.extend(results)

        await BranchJoinFlow().kickoff_async()

        assert received == ["the deck"]


class TestAndJoinOverRoutes:
    """Joining on a multi-route router.

    Observed on execution acb3d8f3: AND correctly waited for both branches, then
    handed the email crew only the politics deck — the branch that finished
    last. The 8,199-char sports deck was discarded, and the email contained zero
    sports content.
    """

    @pytest.mark.asyncio
    async def test_the_join_receives_every_branch_output(self):
        received: list = []

        class JoinAllFlow(Flow):
            @start()
            def classify(self):
                return "classified"

            @router(classify)
            def choose(self, _previous=None):
                return ["route_politics", "route_sports"]

            @listen("route_politics")
            async def politics(self, *results):
                return "politics deck"

            @listen("route_sports")
            async def sports(self, *results):
                return "sports deck"

            @listen(and_(politics, sports))
            async def email(self, *results):
                received.extend(results)

        await JoinAllFlow().kickoff_async()

        assert sorted(received) == ["politics deck", "sports deck"]

    @pytest.mark.asyncio
    async def test_outputs_arrive_in_trigger_order_not_completion_order(self):
        """Otherwise the joining crew sees a different order run to run."""
        received: list = []

        class OrderedFlow(Flow):
            @start()
            def classify(self):
                return "classified"

            @router(classify)
            def choose(self, _previous=None):
                return ["route_b", "route_a"]

            @listen("route_a")
            async def a(self, *results):
                return "a"

            @listen("route_b")
            async def b(self, *results):
                return "b"

            @listen(and_(a, b))
            async def join(self, *results):
                received.extend(results)

        await OrderedFlow().kickoff_async()

        assert received == ["a", "b"]

    @pytest.mark.asyncio
    async def test_a_subset_of_routes_still_satisfies_the_join(self):
        """The hang. AND lists every route listener, but only the routes that
        MATCHED ever run — so a subset used to leave the join unsatisfied and
        the downstream crew silently never ran."""
        ran: list[str] = []

        class SubsetFlow(Flow):
            @start()
            def classify(self):
                return "classified"

            @router(classify)
            def choose(self, _previous=None):
                return ["route_politics"]

            @listen("route_politics")
            async def politics(self, *results):
                return "politics deck"

            @listen("route_sports")
            async def sports(self, *results):
                return "sports deck"

            @listen(and_(politics, sports))
            async def email(self, *results):
                ran.append("email")

        flow = SubsetFlow()
        # The builder records what a router could emit; do the same here.
        SubsetFlow.choose._kasal_routes = ["route_politics", "route_sports"]

        await flow.kickoff_async()

        assert ran == ["email"], "the join must not wait for a route never taken"


class TestDefaultRoute:
    """The engine has always honoured a route named exactly `default`, but the
    canvas could not create one (every name was auto-generated as
    `route_to_<crew>`), so the fallback was dead and a batch matching nothing
    just ended the flow."""

    @pytest.mark.asyncio
    async def test_the_default_branch_runs_when_nothing_matched(self):
        ran: list[str] = []

        class FallbackFlow(Flow):
            @start()
            def classify(self):
                return "classified"

            @router(classify)
            def choose(self, _previous=None):
                return "default"

            @listen("route_politics")
            def politics(self, *results):
                ran.append("politics")

            @listen("default")
            def otherwise(self, *results):
                ran.append("otherwise")

        FallbackFlow.choose._kasal_routes = ["route_politics", "default"]

        await FallbackFlow().kickoff_async()

        assert ran == ["otherwise"]

    @pytest.mark.asyncio
    async def test_a_matching_route_wins_over_the_default(self):
        ran: list[str] = []

        class MatchedFlow(Flow):
            @start()
            def classify(self):
                return "classified"

            @router(classify)
            def choose(self, _previous=None):
                return ["route_politics"]

            @listen("route_politics")
            def politics(self, *results):
                ran.append("politics")

            @listen("default")
            def otherwise(self, *results):
                ran.append("otherwise")

        MatchedFlow.choose._kasal_routes = ["route_politics", "default"]

        await MatchedFlow().kickoff_async()

        assert ran == ["politics"]
