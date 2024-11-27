import groovy.lang.Closure
import org.gradle.plugins.ide.idea.model.IdeaModel
import org.jetbrains.gradle.ext.CopyrightConfiguration
import org.jetbrains.gradle.ext.IdeaExtPlugin

plugins.withType<IdeaExtPlugin> {
    val ideaProject = extensions.getByType<IdeaModel>().project as ExtensionAware
    val settings = ideaProject.extensions.getByName("settings") as ExtensionAware
    val copyright = settings.extensions.getByType<CopyrightConfiguration>()

    copyright.profiles.create("confidential") {
        keyword = "ELASTICSEARCH CONFIDENTIAL"
        notice = """
            ELASTICSEARCH CONFIDENTIAL
            __________________

            Copyright Elasticsearch B.V. All rights reserved.

            NOTICE:  All information contained herein is, and remains
            the property of Elasticsearch B.V. and its suppliers, if any.
            The intellectual and technical concepts contained herein
            are proprietary to Elasticsearch B.V. and its suppliers and
            may be covered by U.S. and Foreign Patents, patents in
            process, and are protected by trade secret or copyright
            law.  Dissemination of this information or reproduction of
            this material is strictly forbidden unless prior written
            permission is obtained from Elasticsearch B.V.
        """.trimIndent()
    }

    // We only want to use the "default" Elastic+SSPL license header for open source code
    copyright.useDefault = "confidential"
    copyright.scopes = mapOf("x-pack" to "Elastic", "llrc" to "Apache2", "elasticsearch" to "Default")

    tasks.named("enablePreviewFeatures") {
        fun enablePreview(moduleFile: String, languageLevel: String) = (extra.get("enablePreview") as Closure<*>).call(moduleFile, languageLevel)

        doLast {
            enablePreview(".idea/modules/elasticsearch/libs/native/elasticsearch.libs.native.main.iml", "JDK_21_PREVIEW")
            enablePreview(".idea/modules/elasticsearch/libs/native/elasticsearch.libs.native.test.iml", "JDK_21_PREVIEW")
        }
    }
}
