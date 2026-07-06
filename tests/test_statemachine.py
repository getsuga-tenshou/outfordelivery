from simulator import statemachine as sm


def test_happy_path_is_valid():
    for src, dst in zip(sm.PRE_DELIVERY, sm.PRE_DELIVERY[1:]):
        assert sm.is_valid_transition(src, dst)
    assert sm.is_valid_transition(sm.OUT_FOR_DELIVERY, sm.DELIVERED)


def test_failure_branch_is_valid():
    assert sm.is_valid_transition(sm.OUT_FOR_DELIVERY, sm.DELIVERY_FAILED)
    assert sm.is_valid_transition(sm.DELIVERY_FAILED, sm.RESCHEDULED)
    assert sm.is_valid_transition(sm.DELIVERY_FAILED, sm.RETURNED)
    assert sm.is_valid_transition(sm.RESCHEDULED, sm.OUT_FOR_DELIVERY)


def test_terminal_states_have_no_successors():
    assert sm.TRANSITIONS[sm.DELIVERED] == set()
    assert sm.TRANSITIONS[sm.RETURNED] == set()
    assert sm.TERMINAL == {sm.DELIVERED, sm.RETURNED}


def test_impossible_transitions_are_rejected():
    assert not sm.is_valid_transition(sm.CREATED, sm.DELIVERED)
    assert not sm.is_valid_transition(sm.DELIVERED, sm.OUT_FOR_DELIVERY)
    assert not sm.is_valid_transition(sm.IN_TRANSIT, sm.DELIVERY_FAILED)
