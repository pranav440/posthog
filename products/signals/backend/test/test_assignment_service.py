from typing import Literal
from uuid import UUID, uuid4

from posthog.test.base import BaseTest
from unittest.mock import patch

from django.test import SimpleTestCase
from django.utils import timezone

from asgiref.sync import async_to_sync, sync_to_async
from parameterized import parameterized
from social_django.models import UserSocialAuth

from posthog.models import OrganizationMembership, User

from products.signals.backend.artefact_attribution import ArtefactAttribution
from products.signals.backend.artefact_schemas import (
    ArtefactContentValidationError,
    ReviewerControl,
    SuggestedReviewerEntry,
    SuggestedReviewers,
    WorkClaim,
)
from products.signals.backend.assignment_flags import assignment_side_effects_allowed, assignment_writer_enabled
from products.signals.backend.assignment_service import (
    AssignmentClaimConflict,
    AssignmentOperationConflict,
    AssignmentProposal,
    AssignmentWriteError,
    ReportAssignmentService,
    StaleAssignmentProposal,
)
from products.signals.backend.models import SignalReport, SignalReportArtefact
from products.signals.backend.report_generation.research import ReportResearchOutput, ResearchReviewerDecision
from products.signals.backend.report_generation.research_reviewers import resolve_research_reviewers
from products.signals.backend.report_generation.select_repo import RepoSelectionResult
from products.signals.backend.scout_harness.tools.report import set_scout_report_reviewers
from products.signals.backend.slack_inbox_notifications import _resolve_suggested_reviewer_user_ids
from products.signals.backend.suggested_reviewer_index import report_ids_naming_reviewers, sync_suggested_reviewer_index
from products.signals.backend.temporal.agentic.report import _persist_agentic_report_artefacts


class TestReportAssignmentService(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.flag = patch("products.signals.backend.assignment_service.assignment_writer_enabled", return_value=True)
        self.flag.start()
        self.addCleanup(self.flag.stop)
        self.effects = patch(
            "products.signals.backend.assignment_service.assignment_side_effects_enabled", return_value=False
        )
        self.effects.start()
        self.addCleanup(self.effects.stop)
        self.report = SignalReport.objects.create(team=self.team, status="ready", title="A report")
        self.service = ReportAssignmentService(team=self.team, report_id=str(self.report.id))
        self.actor = ArtefactAttribution.from_user(self.user.id)
        self.member = User.objects.create(email="reviewer@example.com")
        OrganizationMembership.objects.create(user=self.member, organization=self.organization)
        self.reviewers = (SuggestedReviewerEntry(user_uuid=str(self.member.uuid)),)

    def proposal(
        self, *, outcome: Literal["selected", "no_suitable_owner", "lookup_failed"] = "selected"
    ) -> AssignmentProposal:
        return AssignmentProposal(
            operation_id=uuid4(),
            expected_state=self.service.read().state,
            source="research",
            reviewers=self.reviewers if outcome == "selected" else (),
            outcome=outcome,
            retry_reason="Member lookup unavailable" if outcome == "lookup_failed" else None,
        )

    def test_legacy_human_choice_survives_human_scout_human_sequence(self) -> None:
        SignalReportArtefact.append_status(
            team_id=self.team.id,
            report_id=str(self.report.id),
            content=SuggestedReviewers(root=list(self.reviewers)),
            attribution=self.actor,
            reevaluate_autostart=False,
        )
        chosen = SuggestedReviewers(root=[*self.reviewers, SuggestedReviewerEntry(user_uuid=str(self.user.uuid))])
        self.service.edit_reviewers(chosen, actor=self.actor)
        scout = self.proposal().model_copy(update={"reviewers": ()})
        assert not set_scout_report_reviewers(
            team_id=self.team.id,
            report_id=str(self.report.id),
            suggested_reviewers=SuggestedReviewers(root=[]),
            attribution=ArtefactAttribution.system(),
            proposal=scout,
        )
        assert self.service.read().reviewers == tuple(chosen.root)
        self.service.edit_reviewers(SuggestedReviewers(root=[]), actor=self.actor, removal=True)
        assert self.service.read().control == "human"
        assert not self.service.read().reviewers
        assert not list(
            report_ids_naming_reviewers(
                team_id=self.team.id,
                user_uuids=[str(self.member.uuid)],
                github_logins=[],
                logins_match_unidentified_only=True,
            )
        )

    @parameterized.expand([False, True])
    def test_automatic_first_migration_recovers_human_control(self, automatic_control: bool) -> None:
        SignalReportArtefact.append_status(
            team_id=self.team.id,
            report_id=str(self.report.id),
            content=SuggestedReviewers(root=[]),
            attribution=self.actor,
            reevaluate_autostart=False,
        )
        if automatic_control:
            SignalReportArtefact.append_status(
                team_id=self.team.id,
                report_id=str(self.report.id),
                content=ReviewerControl(mode="automatic"),
                attribution=ArtefactAttribution.system(),
                assignment_writer=True,
            )
        self.service.apply(self.proposal(), actor=ArtefactAttribution.system())
        assert self.service.read().control == "human"
        assert self.service.read().reviewers == ()

    def test_in_flight_proposal_cannot_apply_after_human_edit(self) -> None:
        self.service.edit_reviewers(SuggestedReviewers(root=list(self.reviewers)), actor=self.actor)
        stale = self.proposal()
        self.service.edit_reviewers(SuggestedReviewers(root=[]), actor=self.actor)
        with self.assertRaises(StaleAssignmentProposal):
            self.service.apply(stale, actor=ArtefactAttribution.system())

    @parameterized.expand(["selected", "lookup_failed", "no_suitable_owner"])
    def test_fresh_results_preserve_human_choice(
        self, outcome: Literal["selected", "no_suitable_owner", "lookup_failed"]
    ) -> None:
        self.service.edit_reviewers(SuggestedReviewers(root=list(self.reviewers)), actor=self.actor)
        proposal = self.proposal(outcome=outcome)
        if outcome == "selected":
            proposal = proposal.model_copy(
                update={"reviewers": (SuggestedReviewerEntry(user_uuid=str(self.user.uuid)),)}
            )
        self.service.apply(proposal, actor=ArtefactAttribution.system())
        assert self.service.read().control == "human"
        assert self.service.read().reviewers == self.reviewers

    def test_retries_and_unchanged_proposals_do_not_repeat_reviewer_writes_or_delivery(self) -> None:
        proposal = self.proposal()
        with self.captureOnCommitCallbacks(execute=False):
            first = self.service.apply(proposal, actor=ArtefactAttribution.system())
        with self.captureOnCommitCallbacks(execute=False) as callbacks:
            replay = self.service.apply(proposal, actor=ArtefactAttribution.system(), reevaluate_autostart=True)
            unchanged = self.service.apply(
                self.proposal(), actor=ArtefactAttribution.system(), reevaluate_autostart=True
            )
        assert replay.replayed
        assert not replay.changed and not unchanged.changed
        assert first.reviewers_artefact.id == replay.reviewers_artefact.id == unchanged.reviewers_artefact.id
        assert self.report.artefacts.filter(type="suggested_reviewers").count() == 1
        assert callbacks == []
        with self.assertRaises(AssignmentOperationConflict):
            self.service.apply(
                proposal.model_copy(update={"source": "another-source"}), actor=ArtefactAttribution.system()
            )
        with self.assertRaises(ArtefactContentValidationError):
            first.reviewers_artefact.update_content([])

    def test_access_loss_cleans_legacy_recipient_without_releasing_human_control(self) -> None:
        UserSocialAuth.objects.create(
            user=self.member, provider="github", uid="1234", extra_data={"login": "example-reviewer"}
        )
        SignalReportArtefact.append_status(
            team_id=self.team.id,
            report_id=str(self.report.id),
            content=SuggestedReviewers(root=[SuggestedReviewerEntry(github_login="example-reviewer")]),
            attribution=self.actor,
            reevaluate_autostart=False,
        )
        self.member.is_active = False
        self.member.save(update_fields=["is_active"])
        result = self.service.apply(self.proposal(), actor=ArtefactAttribution.system())
        assert self.service.read().control == "human"
        assert self.service.read().reviewers == ()
        assert self.service.read().human_editor_id == self.user.id
        assert result.decision is not None
        assert result.decision.ineligible_user_uuids == (str(self.member.uuid),)

    def test_human_confirmation_preserves_editor_without_repeating_the_reviewer_row(self) -> None:
        self.service.apply(self.proposal(), actor=ArtefactAttribution.system())
        self.service.edit_reviewers(SuggestedReviewers(root=list(self.reviewers)), actor=self.actor)
        assert self.service.read().control == "human"
        assert self.service.read().human_editor_id == self.user.id
        assert self.report.artefacts.filter(type="suggested_reviewers").count() == 1

        self.member.is_active = False
        self.member.save(update_fields=["is_active"])
        self.service.apply(
            self.proposal().model_copy(update={"action": "cleanup", "reviewers": ()}),
            actor=ArtefactAttribution.system(),
        )
        assert self.service.read().reviewers == ()
        assert self.service.read().control == "human"
        assert self.service.read().human_editor_id == self.user.id

    def test_unresolved_login_is_evidence_not_a_recipient(self) -> None:
        proposal = self.proposal().model_copy(
            update={"reviewers": (SuggestedReviewerEntry(github_login="unlinked-example"),)}
        )
        result = self.service.apply(proposal, actor=ArtefactAttribution.system())
        assert self.service.read().reviewers == ()
        assert result.decision is not None
        assert result.decision.unresolved_candidates[0].github_login == "unlinked-example"

    @parameterized.expand(["access_lost", "unresolved_login"])
    def test_rejected_selection_retains_valid_recipients(self, rejection: str) -> None:
        self.service.apply(self.proposal(), actor=ArtefactAttribution.system())
        if rejection == "access_lost":
            self.user.is_active = False
            self.user.save(update_fields=["is_active"])
            candidate = SuggestedReviewerEntry(user_uuid=str(self.user.uuid))
        else:
            candidate = SuggestedReviewerEntry(github_login="unlinked-example")
        proposal = self.proposal().model_copy(update={"reviewers": (candidate,)})
        result = self.service.apply(proposal, actor=ArtefactAttribution.system())

        assert self.service.read().reviewers == self.reviewers
        assert result.decision is not None and result.decision.outcome == "lookup_failed"
        assert result.decision.retry_reason
        assert not result.changed
        assert self.service.apply(proposal, actor=ArtefactAttribution.system()).replayed

    @parameterized.expand(["selected", "empty", "missing", "rejected", "lookup_failed", "human", "stale"])
    def test_research_selection_controls_inbox_and_notification_recipients(self, selection: str) -> None:
        self.service.apply(self.proposal(), actor=ArtefactAttribution.system())
        if selection == "human":
            self.service.edit_reviewers(SuggestedReviewers(root=[]), actor=self.actor)
        proposal = self.proposal()
        if selection == "stale":
            self.service.edit_reviewers(SuggestedReviewers(root=[]), actor=self.actor)
        decision = ResearchReviewerDecision.model_validate(
            {
                "reviewers": []
                if selection == "empty"
                else [
                    {"user_uuid": str(self.user.uuid), "reason": "Maintains the affected feature."},
                    {"user_uuid": str(self.member.uuid), "reason": "Owns its integration."},
                ],
                "reason": "Current responsibility and related correction evidence.",
            }
        )
        if selection == "rejected":
            decision.reviewers = decision.reviewers[:1]
            self.user.is_active = False
            self.user.save(update_fields=["is_active"])
        result = ReportResearchOutput(
            title="Report", summary="Summary", reviewer_decision=None if selection == "missing" else decision
        )
        with (
            patch("products.signals.backend.temporal.agentic.report.assignment_writer_enabled", return_value=True),
            patch(
                "products.signals.backend.temporal.agentic.report.database_sync_to_async",
                side_effect=lambda func, **kwargs: sync_to_async(func, thread_sensitive=True),
            ),
            patch("products.signals.backend.temporal.agentic.report._build_reviewers_content") as legacy,
            patch("products.signals.backend.temporal.agentic.report.capture_suggested_reviewers_resolved"),
            patch(
                "products.signals.backend.temporal.agentic.report.resolve_research_reviewers",
                side_effect=RuntimeError if selection == "lookup_failed" else None,
                wraps=None if selection == "lookup_failed" else resolve_research_reviewers,
            ),
        ):
            for _ in range(2):
                async_to_sync(_persist_agentic_report_artefacts)(
                    self.team.id,
                    str(self.report.id),
                    result,
                    RepoSelectionResult(repository=None, reason="No repository"),
                    reviewer_proposal=proposal,
                    reviewer_selection_enabled=True,
                )
            legacy.assert_not_called()

        expected = (
            [self.user, self.member]
            if selection == "selected"
            else []
            if selection in {"empty", "human", "stale"}
            else [self.member]
        )
        assert [entry.user_uuid for entry in self.service.read().reviewers] == [str(user.uuid) for user in expected]
        assert _resolve_suggested_reviewer_user_ids(self.report) == {user.id for user in expected}
        for user in [self.user, self.member]:
            assert bool(
                list(
                    report_ids_naming_reviewers(
                        team_id=self.team.id,
                        user_uuids=[str(user.uuid)],
                        github_logins=[],
                        logins_match_unidentified_only=True,
                    )
                )
            ) == (user in expected)
        if selection in {"missing", "rejected", "lookup_failed", "empty", "selected"}:
            saved = self.service.latest_decision()
            assert saved is not None
            assert saved.outcome == (
                "selected"
                if selection == "selected"
                else "no_suitable_owner"
                if selection == "empty"
                else "lookup_failed"
            )
        assert self.report.artefacts.filter(type="assignment_decision").count() == (3 if selection == "human" else 2)

    def test_state_and_index_choose_one_reviewer_list_when_timestamps_match(self) -> None:
        timestamp = timezone.now()
        for number, reviewer in [(1, self.user), (2, self.member)]:
            SignalReportArtefact.objects.create(
                id=UUID(int=number),
                team=self.team,
                report=self.report,
                type="suggested_reviewers",
                content=SuggestedReviewers(
                    root=[SuggestedReviewerEntry(user_uuid=str(reviewer.uuid))]
                ).model_dump_json(),
            )
        self.report.artefacts.update(created_at=timestamp)
        sync_suggested_reviewer_index(team_id=self.team.id, report_id=str(self.report.id))

        assert self.service.read().reviewers == self.reviewers
        assert not list(
            report_ids_naming_reviewers(
                team_id=self.team.id,
                user_uuids=[str(self.user.uuid)],
                github_logins=[],
                logins_match_unidentified_only=True,
            )
        )
        assert list(
            report_ids_naming_reviewers(
                team_id=self.team.id,
                user_uuids=[str(self.member.uuid)],
                github_logins=[],
                logins_match_unidentified_only=True,
            )
        ) == [{"report_id": self.report.id}]

    def test_rollback_stops_automatic_updates_and_keeps_human_edits(self) -> None:
        self.service.apply(self.proposal(), actor=ArtefactAttribution.system())
        with patch("products.signals.backend.assignment_service.assignment_writer_enabled", return_value=False):
            paused = ReportAssignmentService(team=self.team, report_id=str(self.report.id))
            result = paused.apply_compatibility_reviewers(
                SuggestedReviewers(root=[]), actor=ArtefactAttribution.system()
            )
            assert not result.changed
            paused.edit_reviewers(SuggestedReviewers(root=[]), actor=self.actor)
            assert paused.read().control == "human"
            assert paused.read().reviewers == ()
        with self.assertRaises(ArtefactContentValidationError):
            SignalReportArtefact.append_status(
                team_id=self.team.id,
                report_id=str(self.report.id),
                content=SuggestedReviewers(root=list(self.reviewers)),
                attribution=ArtefactAttribution.system(),
            )

    def test_human_cannot_add_a_member_without_project_access(self) -> None:
        self.member.is_active = False
        self.member.save(update_fields=["is_active"])
        with self.assertRaisesMessage(AssignmentWriteError, "access to this project"):
            self.service.edit_reviewers(SuggestedReviewers(root=list(self.reviewers)), actor=self.actor)
        assert not self.report.assignment_revisions.exists()
        assert not self.report.artefacts.exists()

    def test_producer_must_supply_selection_time_state(self) -> None:
        with self.assertRaises(StaleAssignmentProposal):
            self.service.apply_compatibility_reviewers(
                SuggestedReviewers(root=list(self.reviewers)), actor=ArtefactAttribution.system()
            )
        assert not self.report.assignment_revisions.exists()

    def test_claim_is_checked_before_write_and_recorded_on_decision(self) -> None:
        claim = SignalReportArtefact.add_log(
            team_id=self.team.id, report_id=str(self.report.id), content=WorkClaim(), attribution=self.actor
        )
        proposal = self.proposal()
        with self.assertRaises(AssignmentClaimConflict):
            self.service.apply(proposal, actor=ArtefactAttribution.system(), claim_id=str(claim.id))
        result = self.service.apply(proposal, actor=self.actor, claim_id=str(claim.id))
        assert str(result.reviewers_artefact.claim_id) == str(claim.id)
        assert self.report.artefacts.get(type="assignment_decision").claim_id == claim.id

    @parameterized.expand([(False, False), (False, True), (True, False), (True, True)])
    def test_delivery_gate_survives_writer_rollback(self, versioned: bool, effects: bool) -> None:
        if versioned:
            self.service.edit_reviewers(SuggestedReviewers(root=[]), actor=self.actor)
        with (
            patch("products.signals.backend.assignment_flags.assignment_side_effects_enabled", return_value=effects),
            patch("products.signals.backend.assignment_flags.assignment_writer_enabled", return_value=False),
        ):
            assert assignment_side_effects_allowed(team_id=self.team.id, report_id=str(self.report.id)) == (
                not versioned or effects
            )


class TestAssignmentReleaseControls(SimpleTestCase):
    @parameterized.expand([(None, False), (False, False), ("enabled", False), (True, True)])
    def test_writer_requires_explicit_enablement(self, value: object, enabled: bool) -> None:
        with patch("products.signals.backend.assignment_flags.Team.objects") as teams:
            teams.values_list.return_value.get.return_value = "example-org"
            with patch("products.signals.backend.assignment_flags.feature_enabled_or_false", return_value=value):
                assert assignment_writer_enabled(1) is enabled
