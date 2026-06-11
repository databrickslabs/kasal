"""Log-plumbing guards for crewai_patches: raw stderr prints must be gated
to subprocess mode — in the main process they duplicate the logger lines."""

import inspect


def test_kasal_patch_prints_gated_to_subprocess_mode():
    import src.core.crewai_patches as m
    src = inspect.getsource(m)
    gates = src.count('if _os.environ.get("CREW_SUBPROCESS_MODE") == "true":')
    prints = src.count("print(")
    assert prints <= gates, f"{prints} raw print() calls but only {gates} subprocess gates"
