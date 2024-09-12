package gs

type GlueType struct {
	desc       string //类型描述
	scriptFlag bool   //是否为脚本文件
	cmd        string //cmd使用命令
	suffix     string //脚本文件后缀
}

var (
	Bean       = GlueType{desc: "BEAN", scriptFlag: false, cmd: "", suffix: ""}
	GlueShell  = GlueType{desc: "GLUE_SHELL", scriptFlag: true, cmd: "bash", suffix: ".sh"}
	GluePython = GlueType{desc: "GLUE_PYTHON", scriptFlag: true, cmd: "python", suffix: ".py"}
)

func (g *GlueType) GetDesc() string {
	return g.desc
}

func (g *GlueType) GetScriptFlag() bool {
	return g.scriptFlag
}

func (g *GlueType) GetCmd() string {
	return g.cmd
}
