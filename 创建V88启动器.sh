#!/bin/bash

# 创建macOS应用程序包
APP_NAME="AI皇冠双核V88"
APP_DIR="$HOME/Desktop/$APP_NAME.app"
CONTENTS_DIR="$APP_DIR/Contents"
MACOS_DIR="$CONTENTS_DIR/MacOS"
RESOURCES_DIR="$CONTENTS_DIR/Resources"

echo "🚀 正在创建 $APP_NAME 启动器..."

# 创建目录结构
mkdir -p "$MACOS_DIR"
mkdir -p "$RESOURCES_DIR"

# 创建启动脚本
cat > "$MACOS_DIR/launch.sh" << 'LAUNCH'
#!/bin/bash

# 切换到项目目录
cd "$HOME/Desktop/StockAI"

# 打开终端并启动streamlit
osascript << 'APPLESCRIPT'
tell application "Terminal"
    activate
    do script "cd ~/Desktop/StockAI && echo '🎉 AI 皇冠双核 V88 启动中...' && streamlit run app_v88_integrated.py"
end tell
APPLESCRIPT
LAUNCH

chmod +x "$MACOS_DIR/launch.sh"

# 创建Info.plist
cat > "$CONTENTS_DIR/Info.plist" << 'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleExecutable</key>
    <string>launch.sh</string>
    <key>CFBundleIconFile</key>
    <string>AppIcon</string>
    <key>CFBundleIdentifier</key>
    <string>com.stockai.v88</string>
    <key>CFBundleName</key>
    <string>AI皇冠双核V88</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleShortVersionString</key>
    <string>88.0</string>
    <key>CFBundleVersion</key>
    <string>88.0</string>
    <key>LSMinimumSystemVersion</key>
    <string>10.13</string>
    <key>NSHighResolutionCapable</key>
    <true/>
</dict>
</plist>
PLIST

# 创建图标（使用emoji）
cat > "$RESOURCES_DIR/AppIcon.icns" << 'ICON'
👑
ICON

echo "✅ 启动器创建完成！"
echo ""
echo "📍 位置: $APP_DIR"
echo ""
echo "使用方法："
echo "1. 双击桌面上的 '$APP_NAME.app' 启动"
echo "2. 或者将它拖到Dock栏，以后一键启动"
echo ""
echo "🎉 完成！"
