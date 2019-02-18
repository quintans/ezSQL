# Deploy

check if the .m2/settings has the ossrh credentials.

We should use a user token, as described in <https://www.youtube.com/watch?v=b5D2EBjLp40&feature=youtu.be>


```xml
<servers>
   <server>
      <id>ossrh</id>
      <username>username_token</username>
      <password>password_token</password>
   </server>
</servers>
```

```bash
mvn clean deploy -Dmaven.test.skip=true
```

> if the key store is not yet created, follow this video <https://www.youtube.com/watch?v=DE3FVty3NgE&feature=youtu.be>

To retrieve the list of keys type

```bash
gpg --list-secret-keys --keyid-format LONG
```

```bash
gpg2 --keyserver hkp://keyserver.ubuntu.com --send-keys 0F33B0BE4C444D2E
```

after deploy to staging we need to release the deployments.
Follow this <https://central.sonatype.org/pages/releasing-the-deployment.html>

Essentially:

* browse to <https://oss.sonatype.org/#stagingRepositories>
* on the left panel click on **Staging Repositories**
* in the list search for **comgithubquintans-***** and select
* click on **Close**
* (select the **Activity** tab to verify the progress of requirements validation)
* click on **Release**
* wait for 2 Hours :)