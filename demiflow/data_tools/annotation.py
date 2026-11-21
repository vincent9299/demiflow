"""Small composable controls for human review in a Jupyter cell."""
__demiflow_data_tools__ = {"label_picker": "annotation_component"}


def label_picker(labels, on_label):
    """展示候选标签，并在人工点击后把选中的标签交给 on_label 回调。"""
    from ipywidgets import Button, HBox

    if isinstance(labels, str) or not callable(on_label):
        raise ValueError("Provide a sequence of labels and a callable on_label")
    buttons = []
    for label in labels:
        if len(buttons) >= 24 or not isinstance(label, str) or not 0 < len(label) <= 80:
            raise ValueError("Use up to 24 nonempty labels of at most 80 characters")
        button = Button(description=label)
        button.on_click(lambda clicked: on_label(clicked.description))
        buttons.append(button)
    if not buttons:
        raise ValueError("At least one label is required")
    return HBox(buttons)
