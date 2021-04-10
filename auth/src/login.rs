#![allow(unused_imports)]
use log::{info, error, debug};
use std::io::stdout;
use std::io::Write;
use url::Url;

use ate::prelude::*;
use ate::error::LoadError;
use ate::error::TransformError;

use crate::conf_auth;
use crate::prelude::*;
use crate::commands::*;
use crate::service::AuthService;
use crate::helper::*;
use crate::error::*;
use crate::helper::*;

impl AuthService
{
    pub(crate) fn compute_super_key(&self, secret: EncryptKey) -> Option<EncryptKey>
    {
        // Create a session with crypto keys based off the username and password
        let master_key = match self.master_session.read_keys().into_iter().next() {
            Some(a) => a.clone(),
            None => { return None; }
        };
        let super_key = AteHash::from_bytes_twice(master_key.value(), secret.value());
        let super_key = EncryptKey::from_seed_bytes(super_key.to_bytes(), KeySize::Bit256);
        Some(super_key)
    }

    pub async fn process_login<'a>(&self, request: LoginRequest, context: InvocationContext<'a>) -> Result<LoginResponse, ServiceError<LoginFailed>>
    {
        info!("login attempt: {}", request.email);

        let super_key = match self.compute_super_key(request.secret) {
            Some(a) => a,
            None => { return Err(ServiceError::Reply(LoginFailed::NoMasterKey)); }
        };
        let mut session = AteSession::default();
        session.add_read_key(&super_key);

        // Compute which chain the user should exist within
        let user_chain_key = auth_chain_key("auth".to_string(), &request.email);
        let chain = context.repository.open_by_key(&user_chain_key).await?;

        let user_key = PrimaryKey::from(request.email.clone());
        let user =
        {
            // Attempt to load the object (if it fails we will tell the caller)
            let mut dio = chain.dio(&session).await;
            let user = match dio.load::<User>(&user_key).await {
                Ok(a) => a,
                Err(LoadError::NotFound(_)) => {
                    return Err(ServiceError::Reply(LoginFailed::NotFound));
                },
                Err(LoadError::TransformationError(TransformError::MissingReadKey(_))) => {
                    return Err(ServiceError::Reply(LoginFailed::NotFound));
                },
                Err(err) => {
                    return Err(ServiceError::LoadError(err));
                }
            };
            
            // Check if the account is locked or not yet verified
            match user.status {
                UserStatus::Locked => {
                    return Err(ServiceError::Reply(LoginFailed::AccountLocked));
                },
                UserStatus::Unverified => {
                    return Err(ServiceError::Reply(LoginFailed::Unverified));
                },
                UserStatus::Nominal => { },
            };

            // Ok we have the user
            user.take()
        };

        // Add all the authorizations
        let mut session = compute_user_auth(&user, session);

        // If a google authenticator code has been supplied then we need to try and load the
        // extra permissions from elevated rights
        if let Some(code) = request.code {
            let super_super_key = match self.compute_super_key(super_key.clone()) {
                Some(a) => a,
                None => { return Err(ServiceError::Reply(LoginFailed::NotFound)); }
            };
            session.add_read_key(&super_super_key);

            // Load the sudo object
            let mut dio = chain.dio(&session).await;
            if let Some(sudo) = match user.sudo.load(&mut dio).await {
                Ok(a) => a,
                Err(LoadError::NotFound(_)) => {
                    return Err(ServiceError::Reply(LoginFailed::NotFound));
                },
                Err(LoadError::TransformationError(TransformError::MissingReadKey(_))) => {
                    return Err(ServiceError::Reply(LoginFailed::NotFound));
                },
                Err(err) => {
                    return Err(ServiceError::LoadError(err))
                }
            }
            {
                // Check the code matches the authenticator code
                let time = self.ntp_worker.current_timestamp().unwrap();
                let time = time.as_secs() / 30;
                let google_auth = google_authenticator::GoogleAuthenticator::new();
                if google_auth.verify_code(sudo.secret.as_str(), code.as_str(), 3, time) {
                    debug!("code authenticated");
                } else {
                    return Err(ServiceError::Reply(LoginFailed::WrongCode));
                }

                // Add the extra authentication objects from the sudo
                let session = compute_sudo_auth(&sudo.take(), session.clone());

                // Return the session that can be used to access this user
                return Ok(LoginResponse {
                    user_key,
                    nominal_read: user.nominal_read,
                    nominal_write: user.nominal_write,
                    sudo_read: user.sudo_read,
                    sudo_write: user.sudo_write,
                    authority: session.properties.clone()
                });

            } else {
                return Err(ServiceError::Reply(LoginFailed::NotFound));
            }
        }

        // Return the session that can be used to access this user
        Ok(LoginResponse {
            user_key,
            nominal_read: user.nominal_read,
            nominal_write: user.nominal_write,
            sudo_read: user.sudo_read,
            sudo_write: user.sudo_write,
            authority: session.properties.clone()
        })
    }
}

#[allow(dead_code)]
pub async fn login_command(username: String, password: String, code: Option<String>, auth: Url) -> Result<AteSession, LoginError>
{
    // Open a command chain
    let chain_url = crate::helper::command_url(auth);
    let registry = ate::mesh::Registry::new(&conf_auth()).await;
    let chain = registry.open_by_url(&chain_url).await?;

    // Generate a read-key using the password and some seed data
    // (this read-key will be mixed with entropy on the server side to decrypt the row
    //  which means that neither the client nor the server can get at the data alone)
    let prefix = format!("remote-login:{}:", username);
    let read_key = super::password_to_read_key(&prefix, &password, 10);
    
    // Create the login command
    let login = LoginRequest {
        email: username.clone(),
        secret: read_key,
        code,
    };

    // Attempt the login request with a 10 second timeout
    let response: Result<LoginResponse, InvokeError<LoginFailed>> = chain.invoke(login).await;
    match response {
        Err(InvokeError::Reply(LoginFailed::AccountLocked)) => Err(LoginError::AccountLocked),
        Err(InvokeError::Reply(LoginFailed::NotFound)) => Err(LoginError::NotFound(username)),
        Err(InvokeError::Reply(err)) => Err(LoginError::ServerError(err.to_string())),
        result => {
            let mut result = result?;

            let mut session = AteSession::default();
            session.properties.append(&mut result.authority);
            Ok(session)
        }
    }
}

pub async fn load_credentials(username: String, read_key: EncryptKey, _code: Option<String>, auth: Url) -> Result<AteSession, AteError>
{
    // Prepare for the load operation
    let key = PrimaryKey::from(username.clone());
    let mut session = AteSession::new(&conf_auth());
    session.add_read_key(&read_key);

    // Compute which chain our user exists in
    let chain_url = crate::helper::auth_url(auth, &username);

    // Generate a chain key that matches this username on the authentication server
    let registry = ate::mesh::Registry::new(&conf_auth()).await;
    let chain = registry.open_by_url(&chain_url).await?;

    // Load the user
    let mut dio = chain.dio(&session).await;
    let user = dio.load::<User>(&key).await?;

    // Build a new session
    let mut session = AteSession::new(&conf_auth());
    for access in user.access.iter() {
        if let Some(read) = &access.read {
            session.add_read_key(read);
        }
        if let Some(write) = &access.write {
            session.add_write_key(write);
        }
    }
    Ok(session)
}

pub async fn main_login(
    username: Option<String>,
    password: Option<String>,
    code: Option<String>,
    auth: Url
) -> Result<AteSession, LoginError>
{
    let username = match username {
        Some(a) => a,
        None => {
            eprint!("Username: ");
            stdout().lock().flush()?;
            let mut s = String::new();
            std::io::stdin().read_line(&mut s).expect("Did not enter a valid username");
            s
        }
    };

    let password = match password {
        Some(a) => a,
        None => {
            // When no password is supplied we will ask for both the password and the code
            eprint!("Password: ");
            stdout().lock().flush()?;
            let pass = rpassword::read_password().unwrap();

            pass
        }
    };

    // Login using the authentication server which will give us a session with all the tokens
    let session = login_command(username, password, code, auth).await?;
    Ok(session)
}