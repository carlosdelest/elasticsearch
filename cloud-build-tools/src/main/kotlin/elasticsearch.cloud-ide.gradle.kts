import org.gradle.plugins.ide.idea.model.IdeaModel
import org.jetbrains.gradle.ext.CopyrightConfiguration
import org.jetbrains.gradle.ext.IdeaExtPlugin

plugins.withType<IdeaExtPlugin> {
    val ideaProject = extensions.getByType<IdeaModel>().project as ExtensionAware
    val settings = ideaProject.extensions.getByName("settings") as ExtensionAware
    val copyright = settings.extensions.getByType<CopyrightConfiguration>()

    // We only want to use the "default" Elastic+SSPL license header for open source code
    copyright.useDefault = ""
    copyright.scopes = mapOf("x-pack" to "Elastic", "llrc" to "Apache2", "elasticsearch" to "Default")
}
