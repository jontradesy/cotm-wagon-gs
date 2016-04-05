package com.codeonthemind.maven.wagon;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;

import org.apache.maven.wagon.ConnectionException;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authentication.AuthenticationInfo;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.events.SessionEvent;
import org.apache.maven.wagon.events.SessionListener;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.events.TransferListener;
import org.apache.maven.wagon.proxy.ProxyInfo;
import org.apache.maven.wagon.proxy.ProxyInfoProvider;
import org.apache.maven.wagon.repository.Repository;
import org.apache.maven.wagon.resource.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.security.GeneralSecurityException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


/**
 * Google Cloud Storage Wagon Provider implementation.
 *
 * <p>Code and ideas derived from <a href="https://github.com/spring-projects/aws-maven">Spring's 
 * AWS Wagon</a>.</p>
 */
public final class GSWagon
    implements Wagon {

    /** Class level logger instance. */
    private static final Logger LOG = LoggerFactory.getLogger(GSWagon.class);

    private int                         connectionTimeout;
    private Storage                     gs;
    private boolean                     interactive;
    private int                         readTimeout;
    private Repository                  repository;
    private final Set<SessionListener>  sessionListeners;
    private final Set<TransferListener> transferListeners;

    /**
     * Constructs a <code>GSWagon</code>.
     */
    public GSWagon() {
        super();

        this.connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        this.interactive = false;
        this.readTimeout = DEFAULT_READ_TIMEOUT;
        this.sessionListeners = new HashSet<SessionListener>();
        this.transferListeners = new HashSet<TransferListener>();
    }

    /**
     * {@inheritDoc}
     */
    @Override public void addSessionListener(final SessionListener listener) {
        this.sessionListeners.add(checkNotNull(listener, "'listener' cannot be null."));
    }

    /**
     * {@inheritDoc}
     */
    @Override public void addTransferListener(final TransferListener listener) {
        this.transferListeners.add(checkNotNull(listener, "'listener' cannot be null."));
    }

    /**
     * {@inheritDoc}
     */
    @Override public void connect(final Repository source)
        throws ConnectionException, AuthenticationException {
        this.connect(source, null, (ProxyInfoProvider) null);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void connect(final Repository source, final AuthenticationInfo authenticationInfo)
        throws ConnectionException, AuthenticationException {
        this.connect(source, authenticationInfo, (ProxyInfoProvider) null);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void connect(final Repository source, final ProxyInfo proxyInfo)
        throws ConnectionException, AuthenticationException {
        this.connect(source, null, proxyInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void connect(final Repository source, final ProxyInfoProvider proxyInfoProvider)
        throws ConnectionException, AuthenticationException {
        this.connect(source, null, proxyInfoProvider);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void connect(final Repository source, final AuthenticationInfo authenticationInfo,
        final ProxyInfo proxyInfo)
        throws ConnectionException, AuthenticationException {
        this.connect(source, authenticationInfo, new ProxyInfoProvider() {

                @Override public ProxyInfo getProxyInfo(final String protocol) {

                    if ((protocol == null) || (proxyInfo == null)
                            || protocol.equalsIgnoreCase(proxyInfo.getType())) {
                        return (proxyInfo);
                    } else {
                        return (null);
                    }
                }
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override public void connect(final Repository source, final AuthenticationInfo authenticationInfo,
        final ProxyInfoProvider proxyInfoProvider)
        throws ConnectionException, AuthenticationException {
        this.repository = source;
        this.fireSessionOpening();

        try {
            this.gs = this.googleStorage();
            this.fireSessionLoggedIn();
            this.fireSessionOpened();
        } catch (final ConnectionException | AuthenticationException e) {
            this.gs = null;
            this.repository = null;
            this.fireSessionConnectionRefused();
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public void disconnect()
        throws ConnectionException {
        this.fireSessionDisconnecting();
        this.gs = null;
        this.repository = null;
        this.fireSessionLoggedOff();
        this.fireSessionDisconnected();
    }

    /**
     * {@inheritDoc}
     */
    @Override public void get(final String resourceName, final File destination)
        throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {
        final Resource resource = new Resource(checkNotNull(resourceName, "'resourceName' cannot be null."));
        this.fireTransferInitiated(resource, TransferEvent.REQUEST_GET);
        this.fireTransferStarted(resource, TransferEvent.REQUEST_GET);

        try {
            final Storage.Objects.Get request = this.gs.objects().get(this.getBucketName(),
                    this.getFullPath(resourceName));

            request.executeMediaAndDownloadTo(new FileOutputStream(destination) {

                    @Override public void write(int b)
                        throws IOException {
                        super.write(b);
                        fireTransferProgress(resource, TransferEvent.REQUEST_GET, new byte[] { (byte) b }, 1);
                    }

                    @Override public void write(byte[] b)
                        throws IOException {
                        super.write(b);
                        fireTransferProgress(resource, TransferEvent.REQUEST_GET, b, b.length);
                    }

                    @Override public void write(byte[] b, int off, int len)
                        throws IOException {
                        super.write(b, off, len);

                        if (off == 0) {
                            fireTransferProgress(resource, TransferEvent.REQUEST_GET, b, len);
                        } else {
                            final byte[] bytes = new byte[len];
                            System.arraycopy(b, off, bytes, 0, len);
                            fireTransferProgress(resource, TransferEvent.REQUEST_GET, bytes, len);
                        }
                    }
                });

            this.fireTransferCompleted(resource, TransferEvent.REQUEST_GET);
        } catch (final IOException ioe) {
            final TransferFailedException failed = new TransferFailedException(String.format(
                        "Cannot get file '%s'",
                        resourceName), ioe);
            this.fireTransferError(resource, TransferEvent.REQUEST_GET, failed);
            throw failed;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public List<String> getFileList(final String destinationDirectory)
        throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {
        Exception failed = null;

        try {

            final Storage.Objects.List request = this.gs.objects().list(this.getBucketName());
            final List<String>         files = new ArrayList<String>();
            Objects                    objects;
            boolean                    found = false;

            do {
                objects = request.execute();
                found = found || (objects != null);

                if (!found) {
                    throw new ResourceDoesNotExistException(String.format("Cannot find %s", destinationDirectory));
                }

                files.addAll(objects.getItems().stream().map(StorageObject::getName).collect(Collectors.toList()));
                request.setPageToken(objects.getNextPageToken());
            } while (null != objects.getNextPageToken());

            return (files);
        } catch (final IOException ioe) {
            final TransferFailedException ioFailed = new TransferFailedException(String.format(
                        "Cannot get file list for '%s'",
                        destinationDirectory), ioe);
            failed = ioFailed;
            throw ioFailed;
        } catch (final ResourceDoesNotExistException notExist) {
            failed = notExist;
            throw notExist;
        } finally {

            if (failed != null) {
                this.fireTransferError(new Resource(destinationDirectory), TransferEvent.REQUEST_GET, failed);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean getIfNewer(final String resourceName, final File destination, final long timestamp)
        throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {
        boolean   isNewer = false;
        Exception failed = null;

        try {
            final Storage.Objects.Get request = this.gs.objects().get(this.getBucketName(),
                    this.getFullPath(resourceName));
            final StorageObject       object = request.execute();

            if (object == null) {
                throw new ResourceDoesNotExistException(String.format("Cannot find %s", resourceName));
            } else {
                isNewer = object.getTimeCreated().getValue() > timestamp;
            }
        } catch (final IOException ioe) {
            final TransferFailedException ioFailed = new TransferFailedException(String.format(
                        "Cannot get metadata for '%s'",
                        resourceName), ioe);
            failed = ioFailed;
            throw ioFailed;
        } catch (final ResourceDoesNotExistException notExist) {
            failed = notExist;
            throw notExist;
        } finally {

            if (failed != null) {
                this.fireTransferError(new Resource(resourceName), TransferEvent.REQUEST_GET, failed);
            }
        }

        if (isNewer) {
            this.get(resourceName, destination);
        }

        return (isNewer);
    }

    /**
     * {@inheritDoc}
     */
    @Override public int getReadTimeout() {
        return (this.readTimeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override public Repository getRepository() {
        return (this.repository);
    }

    /**
     * {@inheritDoc}
     */
    @Override public int getTimeout() {
        return (this.connectionTimeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean hasSessionListener(final SessionListener listener) {
        return (this.sessionListeners.contains(checkNotNull(listener, "'listener' cannot be null.")));
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean hasTransferListener(final TransferListener listener) {
        return (this.transferListeners.contains(checkNotNull(listener, "'listener' cannot be null.")));
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean isInteractive() {
        return (this.interactive);
    }

    /**
     * {@inheritDoc}
     */
    @Override @Deprecated public void openConnection()
        throws ConnectionException, AuthenticationException {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override public void put(final File source, final String destination)
        throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {
        final Resource resource = new Resource(destination);
        this.fireTransferInitiated(resource, TransferEvent.REQUEST_PUT);
        this.fireTransferStarted(resource, TransferEvent.REQUEST_PUT);

        try {
            final InputStream stream = new FileInputStream(source) {

                    @Override public int read()
                        throws IOException {
                        final int b = super.read();
                        fireTransferProgress(resource, TransferEvent.REQUEST_PUT, new byte[] { (byte) b }, 1);

                        return (b);
                    }

                    @Override public int read(final byte[] b)
                        throws IOException {
                        final int count = super.read(b);
                        fireTransferProgress(resource, TransferEvent.REQUEST_PUT, b, b.length);

                        return (count);
                    }

                    @Override public int read(final byte[] b, final int off, final int len)
                        throws IOException {
                        final int count = super.read(b, off, len);

                        if (off == 0) {
                            fireTransferProgress(resource, TransferEvent.REQUEST_PUT, b, len);
                        } else {
                            final byte[] bytes = new byte[len];
                            System.arraycopy(b, off, bytes, 0, len);
                            fireTransferProgress(resource, TransferEvent.REQUEST_PUT, bytes, len);
                        }

                        return (count);
                    }
                };

            final InputStreamContent contentStream = new InputStreamContent("application/octet-stream", stream);
            final StorageObject      objectMetadata = new StorageObject().setName(this.getFullPath(destination));

            this.gs.objects().insert(
                this.getBucketName(), objectMetadata, contentStream).execute();

            this.fireTransferCompleted(resource, TransferEvent.REQUEST_PUT);
        } catch (final IOException ioe) {
            final TransferFailedException failed = new TransferFailedException(String.format(
                        "Cannot write from '%s' to '%s'",
                        source, destination), ioe);
            this.fireTransferError(resource, TransferEvent.REQUEST_PUT, failed);
            throw failed;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public void putDirectory(final File sourceDirectory, final String destinationDirectory)
        throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException {
        final File[] files = sourceDirectory.listFiles();

        if (files != null) {

            for (File f : files) {
                this.put(f, destinationDirectory + "/" + f.getName());
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public void removeSessionListener(final SessionListener listener) {
        this.sessionListeners.remove(checkNotNull(listener, "'listener' cannot be null."));
    }

    /**
     * {@inheritDoc}
     */
    @Override public void removeTransferListener(final TransferListener listener) {
        this.transferListeners.remove(checkNotNull(listener, "'listener' cannot be null."));
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean resourceExists(final String resourceName)
        throws TransferFailedException, AuthorizationException {

        try {
            final Storage.Objects.Get request = this.gs.objects().get(this.getBucketName(),
                    this.getFullPath(resourceName));
            final StorageObject       object = request.execute();

            return (object != null);
        } catch (final IOException ioe) {
            final TransferFailedException failed = new TransferFailedException(String.format(
                        "Cannot determine if '%s' exists.",
                        resourceName), ioe);
            this.fireTransferError(new Resource(resourceName), TransferEvent.REQUEST_GET, failed);
            throw failed;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public void setInteractive(final boolean interactive) {
        this.interactive = interactive;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void setReadTimeout(final int readTimeout) {
        checkArgument(readTimeout > -1, "'readTimeout' cannot be less than zero.");
        this.readTimeout = readTimeout;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void setTimeout(final int connectionTimeout) {
        checkArgument(connectionTimeout > -1, "'connectionTimeout' cannot be less than zero.");
        this.connectionTimeout = connectionTimeout;
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean supportsDirectoryCopy() {
        return (true);
    }

    /**
     * Notify {@link SessionListener}s that creation of the session's connection was refused
     *
     * @see  org.apache.maven.wagon.events.SessionEvent#SESSION_CONNECTION_REFUSED
     */
    private void fireSessionConnectionRefused() {
        final SessionEvent event = new SessionEvent(this, SessionEvent.SESSION_CONNECTION_REFUSED);
        this.sessionListeners.forEach(listener -> listener.sessionConnectionRefused(event));
    }

    /**
     * Notify {@link SessionListener}s that a session has been disconnected successfully
     *
     * @see  org.apache.maven.wagon.events.SessionEvent#SESSION_DISCONNECTED
     */
    private void fireSessionDisconnected() {
        final SessionEvent event = new SessionEvent(this, SessionEvent.SESSION_DISCONNECTED);
        this.sessionListeners.forEach(listener -> listener.sessionDisconnected(event));
    }

    /**
     * Notify {@link SessionListener}s that a session is being disconnected
     *
     * @see  org.apache.maven.wagon.events.SessionEvent#SESSION_DISCONNECTING
     */
    private void fireSessionDisconnecting() {
        final SessionEvent event = new SessionEvent(this, SessionEvent.SESSION_DISCONNECTING);
        this.sessionListeners.forEach(listener -> listener.sessionDisconnecting(event));
    }

    /**
     * Notify {@link SessionListener}s that an error occurred during while the session was in use
     *
     * @param  exception  The error that occurredh
     */
    private void fireSessionError(Exception exception) {
        final SessionEvent event = new SessionEvent(this, exception);
        this.sessionListeners.forEach(listener -> listener.sessionError(event));
    }

    /**
     * Notify {@link SessionListener}s that the session was logged in successfully
     *
     * @see  org.apache.maven.wagon.events.SessionEvent#SESSION_LOGGED_IN
     */
    private void fireSessionLoggedIn() {
        final SessionEvent event = new SessionEvent(this, SessionEvent.SESSION_LOGGED_IN);
        this.sessionListeners.forEach(listener -> listener.sessionLoggedIn(event));
    }

    /**
     * Notify {@link SessionListener}s that the session was logged off successfully
     *
     * @see  org.apache.maven.wagon.events.SessionEvent#SESSION_LOGGED_OFF
     */
    private void fireSessionLoggedOff() {
        final SessionEvent event = new SessionEvent(this, SessionEvent.SESSION_LOGGED_OFF);
        this.sessionListeners.forEach(listener -> listener.sessionLoggedOff(event));
    }

    /**
     * Notify {@link SessionListener}s that a session has been opened successfully
     *
     * @see  org.apache.maven.wagon.events.SessionEvent#SESSION_OPENED
     */
    private void fireSessionOpened() {
        final SessionEvent event = new SessionEvent(this, SessionEvent.SESSION_OPENED);
        this.sessionListeners.forEach(listener -> listener.sessionOpened(event));
    }

    /**
     * Notify {@link SessionListener}s that a session is being opened
     *
     * @see  org.apache.maven.wagon.events.SessionEvent#SESSION_OPENING
     */
    private void fireSessionOpening() {
        final SessionEvent event = new SessionEvent(this, SessionEvent.SESSION_OPENING);
        this.sessionListeners.forEach(listener -> listener.sessionOpening(event));
    }

    /**
     * Notify {@link TransferListener}s that the transfer was completed successfully
     *
     * @param  resource     The resource being transfered
     * @param  requestType  The type of request executed
     *
     * @see    org.apache.maven.wagon.events.TransferEvent#TRANSFER_COMPLETED
     */
    private void fireTransferCompleted(final Resource resource, final int requestType) {
        final TransferEvent event = new TransferEvent(this, resource, TransferEvent.TRANSFER_COMPLETED, requestType);
        this.transferListeners.forEach(listener -> listener.transferCompleted(event));
    }

    /**
     * Notify {@link TransferListener}s that an error occurred during the transfer
     *
     * @param  resource     The resource being transfered
     * @param  requestType  The type of the request being executed
     * @param  exception    The error that occurred
     */
    private void fireTransferError(final Resource resource, final int requestType, final Exception exception) {
        final TransferEvent event = new TransferEvent(this, resource, exception, requestType);
        this.transferListeners.forEach(listener -> listener.transferError(event));
    }

    /**
     * Notify {@link TransferListener}s that a transfer is being initiated
     *
     * @param  resource     The resource being transfered
     * @param  requestType  The type of request to be executed
     *
     * @see    org.apache.maven.wagon.events.TransferEvent#TRANSFER_INITIATED
     */
    private void fireTransferInitiated(final Resource resource, final int requestType) {
        final TransferEvent event = new TransferEvent(this, resource, TransferEvent.TRANSFER_INITIATED, requestType);
        this.transferListeners.forEach(listener -> listener.transferInitiated(event));
    }

    /**
     * Notify {@link TransferListener}s about the progress of a transfer
     *
     * @param  resource     The resource being transfered
     * @param  requestType  The type of request being executed
     * @param  buffer       The buffer of bytes being transfered
     * @param  length       The length of the data in the buffer
     *
     * @see    org.apache.maven.wagon.events.TransferEvent#TRANSFER_PROGRESS
     */
    private void fireTransferProgress(final Resource resource, final int requestType, final byte[] buffer,
        final int length) {
        final TransferEvent event = new TransferEvent(this, resource, TransferEvent.TRANSFER_PROGRESS, requestType);
        this.transferListeners.forEach(listener -> listener.transferProgress(event, buffer, length));
    }

    /**
     * Notify {@link TransferListener}s that a transfer has started successfully
     *
     * @param  resource     The resource being transfered
     * @param  requestType  The type of request being executed
     *
     * @see    org.apache.maven.wagon.events.TransferEvent#TRANSFER_STARTED
     */
    private void fireTransferStarted(final Resource resource, final int requestType) {
        final TransferEvent event = new TransferEvent(this, resource, TransferEvent.TRANSFER_STARTED, requestType);
        this.transferListeners.forEach(listener -> listener.transferStarted(event));
    }

    /**
     * Returns the current Google Storage bucket name based on {@link #getRepository}.
     *
     * @return  the current Google Storage bucket name based on {@link #getRepository}.
     *
     * @throws  IllegalStateException  thrown if not currently connected.
     */
    private String getBucketName()
        throws IllegalStateException {

        if (this.repository == null) {
            throw new IllegalStateException("Not connected.");
        }

        return (this.repository.getHost());
    }

    /**
     * Returns the full Google Storage path of <code>resource</code> with the current repository's
     * Google Storage bucket.
     *
     * @param   path  The local path to convert to a Google Storage path, never <code>null</code>.
     *
     * @return  the full Google Storage path of <code>resource</code> with the current repository's
     *          Google Storage bucket.
     *
     * @throws  IllegalArgumentException    thrown if <code>resource</code> is <code>null</code>.
     * @throws  IllegalStateException       thrown if not currently connected.
     */
    private String getFullPath(final String resource)
        throws IllegalArgumentException, IllegalStateException {
        checkNotNull(resource, "'resource' cannot be null.");

        if (this.repository == null) {
            throw new IllegalStateException("Not connected.");
        }

        final StringBuilder path = new StringBuilder(this.repository.getBasedir()).deleteCharAt(0);

        if ((path.length() != 0) && (path.charAt(path.length() - 1) != '/')) {
            path.append('/');
        }

        path.append(resource);

        return (path.toString());
    }

    /**
     * Factory for Google Storage client.
     *
     * @return  Google Storage client configured with default credentials.
     *
     * @throws  ConnectionException      thrown if a connection to Google Storage fails.
     * @throws  AuthenticationException  thrown if authentication to Google Storage fails.
     */
    private Storage googleStorage()
        throws ConnectionException, AuthenticationException {

        try {
            final HttpTransport httpTransport = Utils.getDefaultTransport();
            final JsonFactory   jsonFactory = Utils.getDefaultJsonFactory();
            GoogleCredential    credential = GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);

            if (credential.createScopedRequired()) {
                credential = credential.createScoped(StorageScopes.all());
            }

            return (new Storage.Builder(httpTransport, jsonFactory, new RetryHttpInitializerWrapper(credential))
                    .setApplicationName("gs-wagon").build());
        } catch (final Exception e) {
            throw new AuthenticationException("Failed connection to Google Storage.", e);
        }
    }

    /**
     * Automatically retry upon RPC failures, preserving the auto-refresh behavior of the Google
     * Credentials.
     *
     * <p>Taken from <a href="https://cloud.google.com/pubsub/configure">Google Pub/Sub
     * Docs</a>.</p>
     */
    private static class RetryHttpInitializerWrapper
        implements HttpRequestInitializer {

        /** Class level logger instance. */
        private static final Logger LOG = LoggerFactory.getLogger(RetryHttpInitializerWrapper.class);

        /**
         * Intercepts the request for filling in the "Authorization" header field, as well as
         * recovering from certain unsuccessful error codes wherein the Credential must refresh its
         * token for a retry.
         */
        private final Credential wrappedCredential;

        /** A sleeper determining backoff. */
        private final Sleeper sleeper;

        /**
         * Constructs a <code>RetryHttpInitializerWrapper</code> with the specified credentials.
         *
         * @param   wrappedCredentials  Credentials for Authorization header, not <code>null</code>.
         *
         * @throws  IllegalArgumentException  thrown if <code>wrappedCredential</code> is <code>
         *                                    null</code>.
         */
        public RetryHttpInitializerWrapper(final Credential wrappedCredential)
            throws IllegalArgumentException {
            this(wrappedCredential, Sleeper.DEFAULT);
        }

        /**
         * Constructs a <code>RetryHttpInitializerWrapper</code> with the specified credentials.
         *
         * @param   wrappedCredentials  Credentials for Authorization header, not <code>null</code>.
         * @param   sleeper             Determines backoff period, not <code>null</code>.
         *
         * @throws  IllegalArgumentException  thrown if <code>wrappedCredential</code> or <code>
         *                                    sleeper</code> is <code>null</code>.
         */
        public RetryHttpInitializerWrapper(final Credential wrappedCredential, final Sleeper sleeper)
            throws IllegalArgumentException {
            this.wrappedCredential = checkNotNull(wrappedCredential, "'wrappedCredential' cannot be null.");
            this.sleeper = checkNotNull(sleeper, "'sleeper' cannot be null.");
        }

        /**
         * Provide initialization for request.
         *
         * @param   request  The HTTP request to initial, not <code>null</code>.
         *
         * @throws  IllegalArgumentException  thrown if <code>request</code> is <code>null</code>.
         */
        @Override public void initialize(final HttpRequest request)
            throws IllegalArgumentException {
            checkNotNull(request, "'request' cannot be null.");

            request.setReadTimeout(2 * 60000); // 2 minutes read timeout

            final HttpUnsuccessfulResponseHandler backoffHandler =
                new HttpBackOffUnsuccessfulResponseHandler(
                    new ExponentialBackOff())
                .setSleeper(this.sleeper);
            request.setInterceptor(this.wrappedCredential);
            request.setUnsuccessfulResponseHandler(
                new HttpUnsuccessfulResponseHandler() {

                    @Override public boolean handleResponse(HttpRequest request, HttpResponse response,
                        boolean supportsRetry)
                        throws IOException {

                        if (wrappedCredential.handleResponse(
                                    request, response, supportsRetry)) {

                            // If credential decides it can handle it,
                            // the return code or message indicated
                            // something specific to authentication,
                            // and no backoff is desired.
                            return (true);
                        } else if (backoffHandler.handleResponse(
                                    request, response, supportsRetry)) {

                            // Otherwise, we defer to the judgement of
                            // our internal backoff handler.
                            LOG.info("Retrying " + request.getUrl());

                            return (true);
                        } else {
                            return (false);
                        }
                    }
                });
            request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(new ExponentialBackOff())
                .setSleeper(this.sleeper));
        }
    }
}
