package quality_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommentaryScriptScansSourceForMojibake(t *testing.T) {
	root := repositoryRoot(t)
	script := filepath.Join(root, "scripts", "check-commentary.ps1")
	if _, err := os.Stat(script); err != nil {
		t.Fatalf("编码防回归脚本不存在: %v", err)
	}

	cmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script)
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("编码防回归脚本执行失败: %v\n%s", err, string(output))
	}
	if !strings.Contains(string(output), "OK: no mojibake patterns found.") {
		t.Fatalf("脚本输出 = %s, want success summary", string(output))
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("cannot find repository root from %s", dir)
		}
		dir = parent
	}
}
