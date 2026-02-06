@echo off
REM DocGen Frontend - Build and Deploy Script (Windows)
REM This script handles development, production builds, and AWS deployment

setlocal enabledelayedexpansion

REM Configuration
set "SCRIPT_DIR=%~dp0"
set "FRONTEND_DIR=%SCRIPT_DIR%frontend"
set "PROJECT_ROOT=%SCRIPT_DIR%"

REM Colors won't work in cmd, we'll use plain text

echo.
echo ========================================
echo DocGen Frontend - Build and Deploy
echo ========================================
echo.

REM Check prerequisites
:check_prerequisites
where node >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Node.js is not installed
    exit /b 1
)
for /f "tokens=*" %%i in ('node -v') do set NODE_VERSION=%%i
echo [OK] Node.js %NODE_VERSION% found

where npm >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] npm is not installed
    exit /b 1
)
for /f "tokens=*" %%i in ('npm -v') do set NPM_VERSION=%%i
echo [OK] npm %NPM_VERSION% found

if not exist "%FRONTEND_DIR%" (
    echo [ERROR] Frontend directory not found at %FRONTEND_DIR%
    exit /b 1
)
echo [OK] Frontend directory found

REM Parse command
if "%1"=="" goto show_help
if "%1"=="dev" goto dev_server
if "%1"=="build" goto build_production
if "%1"=="install" goto install_dependencies
if "%1"=="type-check" goto type_check
if "%1"=="lint" goto lint_code
if "%1"=="aws-deploy" goto aws_deploy
if "%1"=="help" goto show_help

echo [ERROR] Unknown command: %1
goto show_help

:install_dependencies
echo.
echo ========================================
echo Installing Dependencies
echo ========================================
echo.
cd /d "%FRONTEND_DIR%"
call npm install
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] npm install failed
    exit /b 1
)
echo [OK] Dependencies installed
goto end

:dev_server
echo.
echo ========================================
echo Starting Development Server
echo ========================================
echo.
echo [INFO] Application will be available at http://localhost:3000
echo [INFO] Press Ctrl+C to stop
echo.
cd /d "%FRONTEND_DIR%"
call npm run dev
goto end

:build_production
echo.
echo ========================================
echo Building for Production
echo ========================================
echo.
cd /d "%FRONTEND_DIR%"
call npm run build
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Build failed
    exit /b 1
)
echo [OK] Production build complete
echo [INFO] Build artifacts in: %FRONTEND_DIR%\out
goto end

:type_check
echo.
echo ========================================
echo Running Type Check
echo ========================================
echo.
cd /d "%FRONTEND_DIR%"
call npm run type-check
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Type check failed
    exit /b 1
)
echo [OK] Type check passed
goto end

:lint_code
echo.
echo ========================================
echo Linting Code
echo ========================================
echo.
cd /d "%FRONTEND_DIR%"
call npm run lint
if %ERRORLEVEL% NEQ 0 (
    echo [WARNING] Linting found issues
)
echo [OK] Lint complete
goto end

:aws_deploy
if "%2"=="" (
    echo [ERROR] Usage: %0 aws-deploy ^<bucket-name^> ^<distribution-id^>
    exit /b 1
)
if "%3"=="" (
    echo [ERROR] Usage: %0 aws-deploy ^<bucket-name^> ^<distribution-id^>
    exit /b 1
)

echo.
echo ========================================
echo Deploying to AWS
echo ========================================
echo.

if not exist "%FRONTEND_DIR%\out" (
    echo [ERROR] Build artifacts not found. Run 'npm run build' first
    exit /b 1
)

where aws >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] AWS CLI is not installed
    exit /b 1
)

echo [INFO] S3 Bucket: %2
echo [INFO] CloudFront Distribution: %3
echo.

echo [INFO] Uploading to S3...
cd /d "%FRONTEND_DIR%\out"
call aws s3 sync . "s3://%2/" ^
    --delete ^
    --cache-control "public, max-age=0, must-revalidate" ^
    --exclude "_next/*" ^
    --exclude "static/*"

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] S3 upload failed
    exit /b 1
)

echo [INFO] Uploading assets with long cache...
call aws s3 sync "_next" "s3://%2/_next" ^
    --cache-control "public, max-age=31536000, immutable"

echo [OK] Files uploaded to S3
echo.

echo [INFO] Invalidating CloudFront cache...
call aws cloudfront create-invalidation ^
    --distribution-id "%3" ^
    --paths "/*"

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] CloudFront invalidation failed
    exit /b 1
)

echo [OK] CloudFront invalidated
echo [OK] Deployment complete!
goto end

:show_help
echo.
echo DocGen Frontend - Build and Deploy Script
echo.
echo Usage: %0 ^<command^> [options]
echo.
echo Commands:
echo   dev              Start development server (with hot reload)
echo   build            Full production build pipeline
echo   install          Install dependencies only
echo   type-check       Run TypeScript type checking
echo   lint             Run ESLint
echo   aws-deploy       Build and deploy to AWS S3 + CloudFront
echo                    Usage: %0 aws-deploy ^<bucket-name^> ^<distribution-id^>
echo   help             Show this help message
echo.
echo Examples:
echo   %0 dev
echo   %0 build
echo   %0 aws-deploy my-bucket-name d1234abcd
echo.
echo For more information, see README.md and AWS_DEPLOYMENT_GUIDE.md
echo.
goto end

:end
cd /d "%SCRIPT_DIR%"
endlocal
