#!/bin/bash
# 更新持仓股并同步到GitHub

echo "=================================================="
echo "📊 持仓股更新同步器"
echo "=================================================="

# 检查是否在Git仓库中
if [ ! -d .git ]; then
    echo "❌ 当前目录不是Git仓库"
    echo "提示：请先初始化Git仓库并推送到GitHub"
    exit 1
fi

# 1. 显示当前持仓
echo ""
echo "📋 当前持仓股："
python3 portfolio_manager.py 查看持仓

# 2. 提示用户操作
echo ""
echo "=================================================="
echo "请选择操作："
echo "  1. 添加持仓股"
echo "  2. 删除持仓股"
echo "  3. 更新持仓股"
echo "  4. 同步到GitHub（使用当前持仓）"
echo "  q. 退出"
echo "=================================================="

read -p "请输入选项 [1-4/q]: " choice

case $choice in
    1)
        read -p "股票代码: " code
        read -p "股票名称: " name
        read -p "持仓数量: " quantity
        read -p "买入价格: " price
        
        python3 portfolio_manager.py 添加持仓 $code $name $quantity $price
        
        if [ $? -eq 0 ]; then
            echo ""
            read -p "是否同步到GitHub? [y/n]: " sync
            if [ "$sync" = "y" ]; then
                choice=4
            fi
        fi
        ;;
    
    2)
        read -p "要删除的股票代码: " code
        
        python3 portfolio_manager.py 删除持仓 $code
        
        if [ $? -eq 0 ]; then
            echo ""
            read -p "是否同步到GitHub? [y/n]: " sync
            if [ "$sync" = "y" ]; then
                choice=4
            fi
        fi
        ;;
    
    3)
        read -p "股票代码: " code
        read -p "新数量（留空跳过）: " quantity
        read -p "新价格（留空跳过）: " price
        
        cmd="python3 portfolio_manager.py 更新持仓 $code"
        [ ! -z "$quantity" ] && cmd="$cmd 数量=$quantity"
        [ ! -z "$price" ] && cmd="$cmd 价格=$price"
        
        eval $cmd
        
        if [ $? -eq 0 ]; then
            echo ""
            read -p "是否同步到GitHub? [y/n]: " sync
            if [ "$sync" = "y" ]; then
                choice=4
            fi
        fi
        ;;
    
    4)
        # 同步到GitHub
        ;;
    
    q)
        echo "👋 再见！"
        exit 0
        ;;
    
    *)
        echo "❌ 无效选项"
        exit 1
        ;;
esac

# 如果选择了同步
if [ "$choice" = "4" ]; then
    echo ""
    echo "=================================================="
    echo "🚀 开始同步到GitHub..."
    echo "=================================================="
    
    # 检查是否有远程仓库
    if ! git remote get-url origin > /dev/null 2>&1; then
        echo "❌ 未配置远程仓库"
        echo "请先运行: git remote add origin <你的仓库URL>"
        exit 1
    fi
    
    # 添加修改
    git add my_portfolio.xlsx
    
    # 检查是否有变化
    if git diff --staged --quiet; then
        echo "⚠️  没有检测到持仓股变化"
        exit 0
    fi
    
    # 提交
    commit_time=$(date +"%Y-%m-%d %H:%M:%S")
    git commit -m "chore: 更新持仓股 - $commit_time"
    
    # 推送
    echo ""
    echo "📤 推送到GitHub..."
    git push
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "=================================================="
        echo "✅ 同步成功！"
        echo "=================================================="
        echo ""
        echo "💡 GitHub Actions 会在下次定时运行时使用新的持仓股"
        echo "💡 或者访问GitHub仓库页面手动触发Actions立即运行"
    else
        echo ""
        echo "❌ 推送失败，请检查网络和权限"
        exit 1
    fi
fi
