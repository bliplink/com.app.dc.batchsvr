# deploy_batchsvr_flow

## 用法
- 直接说：`按 deploy_batchsvr_flow 发布`
- 读取参数文件：`src/docs/deploy_batchsvr.properties`

## 发布流程
1. 读取 `host/port/user/password/remote_dir/update_script/local_pkg`
2. 检查远端旧包：`$remote_dir/com.app.dc.batchsvr-0.0.1-SNAPSHOT.jar`
3. 如存在，先备份为：`com.app.dc.batchsvr-0.0.1-SNAPSHOT.jar.bak_yyyyMMdd_HHmmss`
4. 上传新包到：`$remote_dir/com.app.dc.batchsvr-0.0.1-SNAPSHOT.jar`
5. 通过登录 shell 执行更新脚本：`bash -lc '$update_script'`
6. 回传结果：备份文件名、脚本关键输出、进程状态、日志尾部

## 说明
- 覆盖前必须先备份
- 如更新失败，可用最近的 `.bak_yyyyMMdd_HHmmss` 回滚

## 本机发布实现说明
- 当前机器直接使用 `OpenSSH + SSH_ASKPASS` 进行非交互密码发布
- 已验证可用工具：
  - `C:\Windows\System32\OpenSSH\ssh.exe`
  - `C:\Windows\System32\OpenSSH\scp.exe`
- 规则：
  - 按本文件发布时，默认直接使用 `OpenSSH + SSH_ASKPASS`
  - 除非当前方式失效，不再重复检查 `WinSCP/PuTTY/Posh-SSH` 等其他工具

