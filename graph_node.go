package asyncjob

import (
	"fmt"

	"github.com/Azure/go-asyncjob/graph"
)

type stepNode struct {
	StepMeta
}

func newStepNode(sm StepMeta) *stepNode {
	return &stepNode{
		StepMeta: sm,
	}
}

func (sn *stepNode) DotSpec() *graph.DotNodeSpec {
	return &graph.DotNodeSpec{
		ID:        sn.getID(),
		Name:      sn.GetName(),
		Shape:     sn.getShape(),
		Style:     "filled",
		FillColor: sn.getFillColor(),
		Tooltip:   sn.getTooltip(),
	}
}

func (sn *stepNode) getShape() string {
	switch sn.getType() {
	case stepTypeRoot:
		return "triangle"
	case stepTypeParam:
		return "doublecircle"
	case stepTypeTask:
		return "ellipse"
	default:
		return "box"
	}
}

func (sn *stepNode) getFillColor() string {
	switch sn.GetState() {
	case StepStatePending:
		return "gray"
	case StepStateRunning:
		return "yellow"
	case StepStateCompleted:
		return "green"
	case StepStateFailed:
		return "red"
	default:
		return "white"
	}
}

func (sn *stepNode) getTooltip() string {
	state := sn.GetState()
	executionData := sn.ExecutionData()

	if state != StepStatePending && executionData != nil {
		return fmt.Sprintf("Type: %s\\nName: %s\\nState: %s\\nStartAt: %s\\nDuration: %s", string(sn.getType()), sn.GetName(), state, executionData.StartTime, executionData.Duration)
	}

	return fmt.Sprintf("Type: %s\\nName: %s", sn.getType(), sn.GetName())
}

func stepConn(snFrom, snTo *stepNode) *graph.DotEdgeSpec {
	edgeSpec := &graph.DotEdgeSpec{
		FromNodeID: snFrom.getID(),
		ToNodeID:   snTo.getID(),
		Color:      "black",
		Style:      "bold",
	}

	// update edge color, tooltip if NodeTo is started already.
	if snTo.GetState() != StepStatePending {
		executionData := snTo.ExecutionData()
		edgeSpec.Tooltip = fmt.Sprintf("Time: %s", executionData.StartTime)
		fromNodeState := snFrom.GetState()
		if fromNodeState == StepStateCompleted {
			edgeSpec.Color = "green"
		} else if fromNodeState == StepStateFailed {
			edgeSpec.Color = "red"
		}
	}

	return edgeSpec
}
